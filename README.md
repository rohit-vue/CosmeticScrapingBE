# Cosmetic Scraping Backend

FastAPI backend and scraper orchestrator for cosmetic supplier discovery. It exposes API endpoints used by the frontend dashboard, runs scraper scripts in separate Python processes, streams live logs, and stores run CSV outputs under `runs/{run_id}`.

## Requirements

- Python 3.11 or newer recommended
- pip
- Playwright browser binaries
- Google Chrome recommended, because several scrapers prefer the `chrome` Playwright channel
- Optional OpenAI API key for AI supplier filtering
- Optional proxy credentials if proxy scraping is enabled

Python package requirements are pinned in `requirements.txt`.

## Installation

From this backend folder:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m playwright install
```

If you already use the existing `venv` folder, activate that environment instead of creating `.venv`.

## Environment

Create a `.env` file in this folder for secrets or scraper settings. `run_server.py`, `proxy_service.py`, and some scraper helpers load this file automatically.

Common variables:

```env
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o-mini
SCRAPER_MAX_PARALLEL=3
SCRAPER_LOG_VERBOSE=0
SCRAPER_PROXY_ENABLED=1
SCRAPER_PROXY_WARMUP=0
SCRAPER_PROXY_REFRESH_SECONDS=900
SCRAPER_PROXY_TIMEOUT_SECONDS=20
SCRAPER_PROXY_ATTEMPTS=6
SCRAPER_PROXY_BROWSER_RELAUNCHES=4
```

Dashboard run options are passed to scraper processes automatically:

```env
SCRAPER_KEYWORDS=
SCRAPER_COUNTRIES=
SCRAPER_TARGET_SUPPLIERS=
```

Scraper-specific variables used in the code include:

```env
TRADEWHEEL_AUTH_USE_PROXY=0
TRADEWHEEL_STEALTHY_HEADLESS=true
KOMPASS_PLAYWRIGHT_CHANNEL=chrome
KOMPASS_P2_PROFILE_BROWSER=1
KOMPASS_P2_PROFILE_TIMEOUT_MS=90000
MIC_PLAYWRIGHT_CHANNEL=chrome
```

## Running

Start the backend API:

```powershell
python run_server.py
```

The server runs on:

```text
http://localhost:8000
```

Health check:

```text
http://localhost:8000/api/health
```

Expected response:

```json
{"status":"ok"}
```

## Frontend Connection

The frontend should point to this backend URL:

```env
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

If the frontend is running from the sibling `frontendcosmetic` repo, start it in a second terminal:

```powershell
cd ..\frontendcosmetic
npm install
npm run dev
```

Then open:

```text
http://localhost:3000
```

## API Endpoints

- `GET /api/health` - health check
- `GET /api/scrapers` - list registered scrapers and current status
- `POST /api/runs` - start a scraper run
- `GET /api/runs/{run_id}` - get run metadata
- `POST /api/runs/{run_id}/stop` - stop selected scrapers or the run
- `GET /api/runs/{run_id}/state` - stream run state using server-sent events
- `GET /api/runs/{run_id}/scrapers/{scraper_id}/logs` - stream scraper logs
- `GET /api/runs/{run_id}/combined.csv` - download merged cleaned CSV
- `GET /api/runs/{run_id}/scrapers/{scraper_id}/cleaned.csv` - download scraper cleaned CSV
- `GET /api/runs/{run_id}/scrapers/{scraper_id}/raw.csv` - download scraper raw CSV
- `GET /api/runs/{run_id}/scrapers/{scraper_id}/partial.csv` - download scraper partial CSV

## Registered Scrapers

- Tradewheel - `tradewheel_scraper.py`
- Kompass - `kompass_enhanced.py`
- Made-in-China - `made_in_china_scraper_final.py`
- EC21 - `ec21_scraper_final.py`
- ExportPages - `exportpages_scraper_final.py`
- Ensun - `ensun_script.py`
- Europages - `eurpages_scraper.py`

## Output Files

Each API run creates a folder:

```text
runs/{run_id}
```

Typical files include:

- `run.json`
- per-scraper raw CSV files
- per-scraper partial CSV files
- per-scraper cleaned CSV files
- `combined_suppliers.csv`

The backend moves scraper artifacts into the run folder after each scraper process finishes, then rebuilds cleaned outputs and merges them.

## Useful Development Commands

```powershell
pip install -r requirements.txt
python -m playwright install
python run_server.py
```

Run an individual scraper directly from this folder:

```powershell
python tradewheel_scraper.py
```

Use the frontend/API for normal runs because it passes shared options, captures logs, tracks state, and merges outputs.

## Troubleshooting

- If the API is not reachable, confirm `python run_server.py` is still running.
- If browser automation fails, run `python -m playwright install` in the active virtual environment.
- If a scraper cannot launch Chrome, install Google Chrome or clear the scraper-specific Playwright channel setting.
- If logs are too quiet, set `SCRAPER_LOG_VERBOSE=1`.
- If proxy errors block runs, set `SCRAPER_PROXY_ENABLED=0` or update proxy credentials in `.env`.
- If downloads are missing, check the matching `runs/{run_id}` folder and the scraper logs.
