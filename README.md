# AI in Education → Google Sheets (free, scheduled)

Pulls “AI in Education” news from RSS (including Google News queries), scores for relevance, de‑duplicates, and appends to a Google Sheet. Runs locally or on GitHub Actions for $0.

## Features
- Fetches multiple RSS feeds (plus Google News queries)
- Transparent heuristic for AI+Education relevance
- URL normalization to prevent dupes
- Appends rows to a Google Sheet with columns:
  `published_utc, source, title, url, summary, score, tags, id`
- GitHub Actions workflow runs every 30 minutes by default

---

## Quick start (local)

1. **Create a Google Sheet** and copy its Sheet ID (between `/d/` and `/edit` in the URL).
2. **Create a Google Cloud Service Account** and enable **Google Sheets API**.
   - Create a JSON key and download as `service_account.json`.
   - Share the Sheet with the service account’s email as **Editor**.
3. Copy `config.sample.yaml` to `config.yaml` and set:
   - `sheet_id: "<YOUR_SHEET_ID>"`
   - leave `service_account_json: "service_account.json"`
4. Install dependencies and run:
   ```bash
   pip install -r requirements.txt
   python ai_ed_news_to_sheets.py
   ```

Expected output: either “Appended N rows.” or “No new rows met the threshold.”

---

## Run on a schedule (GitHub Actions)

1. Create a **private** GitHub repo and push this project.
2. In the repo: **Settings → Secrets and variables → Actions → New repository secret**  
   - Name: `GOOGLE_SERVICE_ACCOUNT_JSON`  
   - Value: paste the entire JSON contents of your service account key
3. Commit a `config.yaml` to the repo (you can copy `config.sample.yaml`) with your `sheet_id`. For Actions, **do not** include the JSON key in the repo; the workflow writes it at runtime.
4. The included workflow runs every 30 minutes. You can also start it manually: **Actions → AI Ed News to Sheets → Run workflow**.

**No duplicates:** The script canonicalizes URLs and hashes `title + canonical_url`. It also reads existing IDs from the Sheet before appending.

---

## Configuration

Edit `config.yaml`:

- `sheet_id` — required (from your Google Sheet URL)
- `worksheet_title` — optional; default `AI_Ed_News`
- `min_score` — raise to 3 to be stricter
- `feeds` — list of RSS feeds
- `google_news_query` — optional; adds a Google News RSS search
- `keywords_must`, `keywords_nice`, `weights`, `eduish_domains` — tune relevance

Tags are inferred crudely from text (`K-12`, `HigherEd`, `Policy`). Adjust as needed in the script.

---


## Troubleshooting

- **403 / permission errors**: Share your Sheet with the service account email as **Editor**.
- **Nothing gets appended**: Increase `feeds`, lower `min_score`, or verify your Google News query.
- **Duplicates**: Should be rare. If you still see them, they’re likely syndicated stories with different titles or true content twins; you can switch the ID to be URL‑only in `hash_id` if desired.
- **Parallel runs**: The workflow uses `concurrency` to prevent race conditions.

---

## License

MIT License. See `LICENSE`.
