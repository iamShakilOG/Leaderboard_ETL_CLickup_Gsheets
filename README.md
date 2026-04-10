# Leaderboard ETL

This project pulls accepted projects from ClickUp, reads charter and tracker data from Google Sheets, appends delivery and rating data, rebuilds the `final_report` worksheet, and writes ETL logs to the `ETL_Logs` tab.

## What changed

- Non-interactive ETL entrypoint for local runs and GitHub Actions
- Environment-based configuration with validation
- Rotating file logs in `logs/`
- Failure export CSV for skipped and failed projects
- Rate limiting, retry/backoff, and chunked Google Sheets writes to reduce quota pressure
- GitHub Actions workflow prepared for automatic runs every 3 days starting from `2025-06-01`

## Local setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy `env.example` to `.env` and fill in real values.
4. Put the Google service account JSON in `secrets/`.
5. Run:

```bash
python Leaderboard_generate_from_clickup_and_charter.py
```

Optional:

```bash
python Leaderboard_generate_from_clickup_and_charter.py --force-run
python Leaderboard_generate_from_clickup_and_charter.py --start-date 2025-06-01 --end-date 2026-12-31
```

## GitHub Actions secrets

Create these repository secrets before pushing:

- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `DELIVERY_SHEET_KEY`
- `GOOGLE_SERVICE_ACCOUNT_JSON`

`GOOGLE_SERVICE_ACCOUNT_JSON` must contain the full JSON content of the service account key, not a file path.

## Scheduling note

GitHub Actions cron cannot express a perfect "every 3 days from June 1, 2025" schedule. The workflow runs daily at `03:15 UTC`, and the ETL script itself decides whether the current date matches the 3-day cadence starting from `2025-06-01`.
