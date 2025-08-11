import hashlib
import re
import sys
import time
import yaml
import feedparser
from urllib.parse import quote, urlparse, urlunparse
from datetime import datetime, timezone
from dateutil import parser as dtparse
from datetime import timedelta



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

# --------------- Runner ---------------

def run():
    cfg = load_config()
    ws = connect_sheet(cfg)
    seen_ids = get_seen_ids(ws)
    min_score = int(cfg.get("min_score", 2))
    feeds = build_feeds(cfg)
    print(f"Pulling {len(feeds)} feeds...")

    # pull these once
    max_age_days = int(cfg.get("max_age_days", 7))
    allow_undated = bool(cfg.get("allow_undated", False))
    exclude_domains = [d.lower() for d in (cfg.get("exclude_domains") or [])]
    exclude_patterns = cfg.get("exclude_patterns") or []
    require_edu_term = bool(cfg.get("require_edu_term", True))
    edu_terms = cfg.get("keywords_nice", [])

    new_rows = []
    for url in feeds:
        try:
            parsed = feedparser.parse(url)
        except Exception as e:
            print(f"Feed error {url}: {e}")
            continue

        for entry in parsed.entries:
            title = normalize_text(entry.get("title"))
            link_raw = entry.get("link") or entry.get("id") or ""
            link = canonical_link(link_raw)
            summary = normalize_text(entry.get("summary") or entry.get("description") or "")
            published_dt = parse_published(entry)
            domain = source_domain(link)
            src = domain or normalize_text(parsed.feed.get("title", "")) or "unknown"

            # 1) recency gate
            if not is_recent(published_dt, max_age_days, allow_undated):
                continue

            # 2) domain/text excludes
            if domain and any(d in domain for d in exclude_domains):
                continue
            low_text = f"{title} {summary}"
            if any(re.search(pat, low_text, flags=re.I) for pat in exclude_patterns):
                continue

            # 3) require clear edu term (separate from AI must-terms)
            if require_edu_term and not contains_term(low_text, edu_terms):
                continue

            # 4) score relevance (no published_dt param; your function doesn't take it)
            s = score_relevance(title, summary, domain, cfg)
            if s < min_score:
                continue

            # 5) dedupe
            _id = hash_id(title, link)
            if _id in seen_ids:
                continue

            # 6) published timestamp string
            if published_dt:
                published_utc = published_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            else:
                published_utc = ""

            # 7) simple tags
            low = low_text.lower()
            tags = []
            if "k-12" in low or "k12" in low:
                tags.append("K-12")
            if "higher" in low or "university" in low or "college" in low:
                tags.append("HigherEd")
            if "policy" in low or "regulation" in low:
                tags.append("Policy")

            new_rows.append([published_utc, src, title, link, summary, str(s), ",".join(tags), _id])

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW", table_range="A1")
        print(f"Appended {len(new_rows)} rows.")
    else:
        print("No new rows met the threshold.")


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
