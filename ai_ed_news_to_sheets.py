import hashlib
import re
import sys
import time
import yaml
import feedparser
from datetime import datetime, timezone
from dateutil import parser as dtparse
from datetime import timedelta
import requests
import trafilatura
import string
from difflib import SequenceMatcher
from urllib.parse import quote, urlparse, urlunparse, unquote, parse_qs
import html as htmllib





import gspread
from google.oauth2.service_account import Credentials

# --------------- Config / Sheets ---------------

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def connect_sheet(cfg):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    keyfile = cfg.get("service_account_json", "service_account.json")
    creds = Credentials.from_service_account_file(keyfile, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(cfg["sheet_id"])
    title = cfg.get("worksheet_title", "AI_Ed_News")
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=8)
    ensure_header(ws)
    return ws

def ensure_header(ws):
    header = ["published_utc", "source", "title", "url", "summary", "score", "tags", "id"]
    existing = ws.get_all_values()
    if not existing:
        ws.append_row(header, value_input_option="RAW")
        return
    if existing[0] != header:
        ws.delete_rows(1)
        ws.append_row(header, value_input_option="RAW")

def get_seen_ids(ws):
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return set()
    header = vals[0]
    idx = header.index("id")
    return {row[idx] for row in vals[1:] if len(row) > idx and row[idx]}

# --------------- RSS + Scoring ---------------

def google_news_rss(query: str) -> str:
    if not query: return ""
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"

def build_feeds(cfg):
    feeds = list(cfg.get("feeds", []) or [])
    q = cfg.get("google_news_query", "")
    if q:
        feeds.append(google_news_rss(q))
    uniq, seen = [], set()
    for f in feeds:
        u = f.strip()
        if u and u not in seen:
            uniq.append(u); seen.add(u)
    return uniq

def normalize_text(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def parse_published(entry):
    for k in ["published", "updated", "pubDate"]:
        if k in entry:
            try:
                return dtparse.parse(entry[k])
            except Exception:
                pass
    if "published_parsed" in entry and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except Exception:
            pass
    # don't pretend undated items are "now"
    return None

def canonical_link(url: str) -> str:
    if not url:
        return url
    u = urlparse(url)
    path = u.path.rstrip("/") or "/"
    clean = (u.scheme, u.netloc.lower(), path, "", "", "")
    return urlunparse(clean)

def source_domain(link: str) -> str:
    try:
        return urlparse(link).netloc.lower()
    except Exception:
        return ""

def hash_id(title, link):
    h = hashlib.sha256()
    h.update((title or "").encode("utf-8"))
    h.update((link or "").encode("utf-8"))
    return h.hexdigest()[:16]

def contains_term(text: str, terms) -> bool:
    text_l = (text or "").lower()
    for t in (terms or []):
        t = (t or "").lower().strip()
        if not t:
            continue
        # word-boundary match so "ai" doesn't hit "chair"
        if re.search(rf"\b{re.escape(t)}\b", text_l):
            return True
    return False

UA = {"User-Agent": "Mozilla/5.0 (compatible; AI-Ed-NewsBot/1.0)"}


def _is_googleish(u: str) -> bool:
    try:
        host = urlparse(u).netloc.lower()
    except Exception:
        return False
    return ("news.google." in host) or host.endswith("google.com") or host.endswith("googleusercontent.com")

def _unwrap_url_param(u: str) -> str:
    try:
        q = parse_qs(urlparse(u).query)
        if "url" in q and q["url"]:
            return unquote(q["url"][0])
    except Exception:
        pass
    return u

def pick_publisher_url_from_entry(entry) -> str:
    """
    For Google News items, try to find the real publisher URL from description or link params.
    Works for most GN RSS variants without extra deps.
    """
    candidates = []

    # 1) Try description: often contains <a href="https://publisher...">
    desc = entry.get("summary") or entry.get("description") or ""
    if desc:
        desc = htmllib.unescape(desc)
        hrefs = re.findall(r'href="(https?://[^"]+)"', desc)
        candidates.extend(hrefs)

    # 2) Try link rels
    for l in (entry.get("links") or []):
        href = l.get("href")
        if href:
            candidates.append(href)

    # 3) Fallback: raw link
    if entry.get("link"):
        candidates.append(entry["link"])

    # Clean, unwrap, and filter
    cleaned = []
    for u in candidates:
        u = _unwrap_url_param(u)
        u = unquote(u)
        if u and not _is_googleish(u):
            cleaned.append(u)

    # Prefer AMP or obvious article-looking URLs
    if cleaned:
        cleaned.sort(key=lambda u: (0 if ("/amp" in u or urlparse(u).netloc.startswith("amp.")) else 1, -len(u)))
        return cleaned[0]

    # Nothing non-Google found; return raw link so the next step can still try
    return entry.get("link") or ""


def _canonical_from_html(html: str) -> str | None:
    if not html:
        return None
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\'](https?://[^"\']+)["\']', html, re.I)
    return m.group(1) if m else None

def _unwrap_googleish(url: str, html: str = "") -> str:
    """Turn Google News/AMP/redirect URLs into the real publisher URL if possible."""
    try:
        u = urlparse(url)
    except Exception:
        return url
    host = u.netloc.lower()

    # 1) Redirect params: ?url= / ?q= / ?u= / ?dest=
    qs = parse_qs(u.query or "")
    for key in ("url", "q", "u", "dest"):
        if key in qs:
            cand = unquote(qs[key][0])
            if cand.startswith("http"):
                return cand

    # 2) Google AMP Viewer: https://www.google.com/amp(/s)/<publisher>/<path>
    if host.endswith("google.com") and u.path.startswith("/amp"):
        path = u.path.split("/amp/", 1)[-1]
        if path.startswith("s/"):
            path = path[2:]
        return f"https://{path}"

    # 3) HTML canonical tag
    canon = _canonical_from_html(html)
    if canon:
        return canon

    # 4) First non-Google absolute link in the page (prefer AMP links)
    if html:
        hrefs = re.findall(r'href=["\'](https?://[^"\']+)["\']', html, flags=re.I)
        for h in hrefs:
            if not _is_googleish(h):
                return h

    return url



def fetch_lede_and_final_url(url: str, timeout: int = 8, max_chars: int = 600):
    """
    Follow redirects; if we still land on a Google page, we won't extract text.
    Returns (final_url, first_paragraph_or_empty).
    """
    final_url, lede = url, ""
    try:
        r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
        final_url = r.url or url
        html = r.text if r.status_code == 200 else ""

        text = None
        if html and not _is_googleish(final_url):
            text = trafilatura.extract(html, include_comments=False, include_tables=False, favor_precision=True)
        if not text and not _is_googleish(final_url):
            downloaded = trafilatura.fetch_url(final_url, timeout=timeout)
            if downloaded:
                text = trafilatura.extract(downloaded, include_comments=False, include_tables=False, favor_precision=True)

        if text:
            paras = [p.strip() for p in text.split("\n") if p.strip()]
            if paras:
                lede = paras[0]
                if len(lede) > max_chars:
                    lede = lede[:max_chars].rstrip() + "…"
    except Exception:
        pass
    return final_url, lede





def is_recent(published_dt: datetime, max_age_days: int, allow_undated: bool) -> bool:
    if not published_dt:
        return bool(allow_undated)
    now = datetime.now(timezone.utc)
    age = now - published_dt.astimezone(timezone.utc)
    return age <= timedelta(days=max_age_days)

def score_relevance(title, summary, domain, cfg):
    title_l = (title or "").lower()
    summary_l = (summary or "").lower()

    must = [w.lower() for w in cfg.get("keywords_must", [])]
    nice = [w.lower() for w in cfg.get("keywords_nice", [])]
    weights = cfg.get("weights", {})
    w_title = int(weights.get("title_keyword", 2))
    w_sum = int(weights.get("summary_keyword", 1))
    w_src = int(weights.get("source_bonus_edu", 1))

    # Hard gate: at least one must-word present
    if must and not any(w in title_l or w in summary_l for w in must):
        return 0

    score = 0
    score += sum(w_title for w in nice if w in title_l)
    score += sum(w_sum for w in nice if w in summary_l)

    eduish = [d.lower() for d in cfg.get("eduish_domains", [])]
    if domain and any(d in domain for d in eduish):
        score += w_src
    return score

def upsert_readme(spreadsheet, cfg, stats):
    """
    Create/update a README tab explaining what this sheet does.
    stats = {"appended": int, "feeds_count": int, "min_score": int, "max_age_days": int}
    """
    title = str(cfg.get("readme_title", "README"))
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=200, cols=4)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    feeds_count = stats.get("feeds_count", 0)
    min_score = stats.get("min_score", cfg.get("min_score", 2))
    max_age_days = stats.get("max_age_days", cfg.get("max_age_days", 7))
    appended = stats.get("appended", 0)

    # Keep it simple: two columns, wrapped text
    lines = [
        ["AI in Education News · README", ""],
        ["Last run (UTC)", now_utc],
        ["What this does",
         ("• Pulls recent AI + Education headlines from RSS (incl. Google News search)\n"
          "• Filters by recency (≤ {d} days) and relevance (min_score ≥ {s})\n"
          "• De-duplicates by title + canonical URL\n"
          "• Writes rows to the 'AI_Ed_News' tab").format(d=max_age_days, s=min_score)],
        ["Columns",
         "published_utc · source · title · url · summary · score · tags · id"],
        ["Sources",
         f"{feeds_count} feeds configured in config.yaml (see `feeds` and `google_news_query`)."],
        ["Relevance rules",
         ("Must include at least one of: {must}\n"
          "Education hints: {nice}\n"
          "Source bonus domains: {eduish}").format(
             must=", ".join(cfg.get("keywords_must", [])) or "—",
             nice=", ".join(cfg.get("keywords_nice", [])) or "—",
             eduish=", ".join(cfg.get("eduish_domains", [])) or "—",
         )],
        ["Recency & filters",
         ("max_age_days: {d}\n"
          "require_edu_term: {r}\n"
          "allow_undated: {u}\n"
          "exclude_domains: {ed}\n"
          "exclude_patterns: {ep}").format(
             d=max_age_days,
             r=bool(cfg.get("require_edu_term", True)),
             u=bool(cfg.get("allow_undated", False)),
             ed=", ".join(cfg.get("exclude_domains", [])) or "—",
             ep=", ".join(cfg.get("exclude_patterns", [])) or "—",
         )],
        ["Lede extraction",
         ("fetch_article_text: {fat}\n"
          "prefer_lede_over_rss: {pl}\n"
          "article_timeout_secs: {t}\n"
          "article_max_chars: {m}").format(
             fat=bool(cfg.get("fetch_article_text", True)),
             pl=bool(cfg.get("prefer_lede_over_rss", True)),
             t=int(cfg.get("article_timeout_secs", 6)),
             m=int(cfg.get("article_max_chars", 600)),
         )],
        ["Latest run result", f"Appended {appended} new rows."],
        ["Maintenance",
         ("• To change sources: edit `config.yaml` → feeds / google_news_query\n"
          "• To adjust strictness: raise `min_score` or lower it if too quiet\n"
          "• To change recency: set `max_age_days`\n"
          "• To pause: disable the GitHub Action in the repo’s Actions tab")],
    ]

    ws.clear()
    ws.update("A1", lines, value_input_option="RAW")

    # Pretty up: wrap text, widen columns, bold title
    try:
        ws.format("A1:B1", {"textFormat": {"bold": True, "fontSize": 14}})
        ws.format("A1:B200", {"wrapStrategy": "WRAP"})
        # widen column B
        spreadsheet.batch_update({
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": 1,
                            "endIndex": 2
                        },
                        "properties": {"pixelSize": 700},
                        "fields": "pixelSize"
                    }
                }
            ]
        })
    except Exception:
        # Formatting failures are non-fatal; content is what matters.
        pass

# --------------- Deduplication ---------------
def normalize_title_for_dedupe(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    # kill punctuation, collapse spaces
    table = str.maketrans({c: " " for c in string.punctuation})
    s = s.translate(table)
    s = re.sub(r"\s+", " ", s).strip()
    # optional: trim boilerplate words at ends
    s = re.sub(r"\b(opinion|analysis|sponsored)\b$", "", s).strip()
    return s

def get_existing_dedupe_maps(ws):
    """
    Read existing rows and return:
      - seen_ids: set of 'id'
      - seen_pairs: set of (norm_title, domain)
      - titles_by_domain: dict[domain] -> list of norm_title
    """
    vals = ws.get_all_values()
    seen_ids, seen_pairs = set(), set()
    titles_by_domain = {}
    if not vals or len(vals) < 2:
        return seen_ids, seen_pairs, titles_by_domain

    header = vals[0]
    idx_id = header.index("id")
    idx_t  = header.index("title")
    idx_u  = header.index("url")

    for row in vals[1:]:
        if len(row) <= max(idx_id, idx_t, idx_u):
            continue
        _id = row[idx_id]
        t = normalize_title_for_dedupe(row[idx_t])
        d = source_domain(row[idx_u] or "")
        if _id:
            seen_ids.add(_id)
        if t and d:
            seen_pairs.add((t, d))
            titles_by_domain.setdefault(d, []).append(t)
    return seen_ids, seen_pairs, titles_by_domain

# --------------- Runner ---------------

def run():
    cfg = load_config()
    ws = connect_sheet(cfg)
    seen_ids, seen_pairs, titles_by_domain = get_existing_dedupe_maps(ws)
    # also track what we add this run so we don't double-append in the same batch
    added_ids = set()
    added_pairs = set()
    added_titles_by_domain = {}

    min_score = int(cfg.get("min_score", 2))
    feeds = build_feeds(cfg)
    print(f"Pulling {len(feeds)} feeds...")

    # Config knobs (with defaults)
    max_age_days = int(cfg.get("max_age_days", 7))
    allow_undated = bool(cfg.get("allow_undated", False))
    exclude_domains = [d.lower() for d in (cfg.get("exclude_domains") or [])]
    exclude_patterns = cfg.get("exclude_patterns") or []
    require_edu_term = bool(cfg.get("require_edu_term", True))
    edu_terms = cfg.get("keywords_nice", [])

    fetch_article_text = bool(cfg.get("fetch_article_text", True))
    prefer_lede_over_rss = bool(cfg.get("prefer_lede_over_rss", True))
    article_timeout_secs = int(cfg.get("article_timeout_secs", 6))
    article_max_chars = int(cfg.get("article_max_chars", 600))
    rewrite_link_to_final = bool(cfg.get("rewrite_link_to_final", True))

    new_rows = []
    for url in feeds:
        try:
            parsed = feedparser.parse(url)
        except Exception as e:
            print(f"Feed error {url}: {e}")
            continue

        for entry in parsed.entries:
            title = normalize_text(entry.get("title"))
            summary_rss = normalize_text(entry.get("summary") or entry.get("description") or "")
            published_dt = parse_published(entry)

            # 1) Recency gate
            if not is_recent(published_dt, max_age_days, allow_undated):
                continue

            # 2) Find best candidate URL (escape Google News)
            candidate_url = pick_publisher_url_from_entry(entry)

            # 3) Resolve to final URL + try lede
            final_url, lede = (candidate_url, "")
            if fetch_article_text and candidate_url:
                final_url, lede = fetch_lede_and_final_url(
                    candidate_url, timeout=article_timeout_secs, max_chars=article_max_chars
                )

            # 4) Canonicalize the URL we’ll store
            link_for_sheet = final_url if rewrite_link_to_final and final_url else candidate_url
            link_canon = canonical_link(link_for_sheet)
            domain = source_domain(link_canon)
            src = domain or normalize_text(parsed.feed.get("title", "")) or "unknown"

            # 5) Excludes and edu-term
            if domain and any(d in domain for d in exclude_domains):
                continue
            low_text = f"{title} {summary_rss}"
            if any(re.search(pat, low_text, flags=re.I) for pat in exclude_patterns):
                continue
            if require_edu_term and not contains_term(low_text, edu_terms):
                continue

            # 6) Score with RSS text (stable)
            s = score_relevance(title, summary_rss, domain, cfg)
            if s < min_score:
                continue

            # 7) Dedup
            _id = hash_id(title, link_canon)
            if _id in seen_ids:
                continue

            # 8) Timestamp
            published_utc = (
                published_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                if published_dt else ""
            )

            # 9) Tags (+ mark summary source)
            tags = []
            low = low_text.lower()
            if "k-12" in low or "k12" in low:
                tags.append("K-12")
            if "higher" in low or "university" in low or "college" in low:
                tags.append("HigherEd")
            if "policy" in low or "regulation" in low:
                tags.append("Policy")
            use_lede = bool(lede) and prefer_lede_over_rss
            tags.append("src:LEDE" if use_lede else "src:RSS")

            # useful warning if we’re still stuck on Google News
            if _is_googleish(link_canon):
                print(f"[WARN] Stuck on Google News: {title[:80]} -> {link_canon}")

            # 10) Choose summary and append
            summary_out = lede if use_lede else summary_rss
            new_rows.append([published_utc, src, title, link_canon, summary_out, str(s), ",".join(tags), _id])

    appended = 0
    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW", table_range="A1")
        appended = len(new_rows)
        print(f"Appended {appended} rows.")
    else:
        print("No new rows met the threshold.")

    # Optional: update README tab if you added upsert_readme()
    if bool(cfg.get("readme_enabled", True)):
        stats = {
            "appended": appended,
            "feeds_count": len(feeds),
            "min_score": min_score,
            "max_age_days": max_age_days,
        }
        try:
            upsert_readme(ws.spreadsheet, cfg, stats)
        except Exception as e:
            print(f"README update skipped: {e}")




if __name__ == "__main__":
    for attempt in range(2):
        try:
            run()
            break
        except Exception as e:
            print(f"Run failed: {e}")
            time.sleep(2)
            if attempt == 1:
                sys.exit(1)
