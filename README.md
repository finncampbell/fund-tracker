# Fund Tracker

This repository now has two distinct parts:

1. **Static Dashboard → GitHub Pages**  
   - All HTML/CSS/JS and CSV/JSON data live under the `docs/` directory.  
   - GitHub Pages publishes the `docs/` folder directly; no build step required.  
   - `docs/index.html` loads `/assets/data/*.csv` and `/assets/data/directors.json` for display.

2. **Serverless Backfill → Netlify Functions**  
   - Netlify is used **only** to host the `backfill.js` function under `netlify/functions/`.  
   - No static build or deploy—`netlify.toml`’s build command is a no-op.  
   - Environment variables (e.g. `CH_API_KEY`, `GITHUB_DISPATCH_TOKEN`) are set in the Netlify UI.

## CI Workflows

- **.github/workflows/fund-tracker.yml**  
  - Runs every 10 minutes (plus manual dispatch) to:
    1. Ingest new companies (`fund_tracker.py`).
    2. Fetch up to today’s “relevant” directors (`fetch_directors.py`).
  - Commits updated CSV/JSON under `docs/assets/data/`.

- **.github/workflows/update-data.yml**  
  - Optional hourly full rebuild of all data.

## Key Scripts

- **`fund_tracker.py`**  
  - Safe pagination (via `total_results`), SIC enrichment, regex classification.  
  - Writes `master_companies.csv/.xlsx` and filtered `relevant_companies.csv/.xlsx`.

- **`fetch_directors.py`**  
  - Dynamically batches against shared rate limit.  
  - Fetches directors for every entry in `relevant_companies.csv`.  
  - Writes `directors.json`.

- **`backfill_directors.py`**  
  - Accepts `--start_date`/`--end_date`.  
  - Dynamic batch backfill, writes `backfill_status.json` for UI progress.  
  - Updates `directors.json`.

- **`rate_limiter.py`**  
  - Implements a 600-calls/5-min sliding-window with a 50-call buffer (effective cap 550).  
  - Shared JSON state in `assets/logs/rate_limit.json`.

- **`logger.py`**  
  - Centralized logging configuration to `assets/logs/*.log`.

## UI Behavior

- **Filter Tabs**:  
  - “All” shows all *relevant* (Category ≠ Other).  
  - Dedicated tabs for Ventures, Capital, Equity, Advisors, Partners, Investments.  
  - “Fund Entities” only shows LLP, LP, GP, Fund.

- **SIC Table**:  
  - Separate view for entries with SIC matches.  

- **Backfill Control**:  
  - Flatpickr date-range picker; posts to Netlify Function.  
  - Progress shown via polling `backfill_status.json`.

## Rate Limiting & Concurrency

- Shared across all scripts; prevents 429s.  
- Logs permit debugging of sleeps, prunes, and remaining quota.

---

Enjoy your streamlined Fund Tracker setup!  
