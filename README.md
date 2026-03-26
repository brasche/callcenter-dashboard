# Call Center Dashboard

Real-time call center metrics dashboard. Pulls live agent activity from the BlueRock telephony API and deal enrollment data from Google Sheets, stores everything in SQLite, and presents a live dashboard with per-agent KPIs, call recordings, and an agent timeline.

## Features

- Per-agent metrics: inbound calls, talk time, call pickup speed, deals, CPS (calls per sale)
- Agent state timeline: logged-in, on-call, paused, wrap-up blocks with accurate start/end times
- Call recordings: one-click download via BlueRock API
- Google Sheets deal sync: auto-matches agents by name
- DB viewer: inspect all tables via `/db-viewer.html`
- Agent roster management via `/agents.html`

---

## Running Locally

### 1. Clone and install

```bash
git clone <your-repo-url>
cd callcenter-dashboard
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and fill in your values (see Environment Variables below)
```

For Google Sheets locally, place your `credentials.json` service account key in the project root and set `GOOGLE_SHEETS_CREDENTIALS_FILE=credentials.json` in `.env`.

### 3. Run

```bash
uvicorn main:app --reload
```

Open [http://localhost:8000](http://localhost:8000).

---

## Environment Variables

Copy `.env.example` to `.env` and fill in every value. Required variables:

| Variable | Description |
|---|---|
| `BLUEROCK_API_KEY` | BlueRock API bearer token |
| `BLUEROCK_API_URL` | BlueRock base URL (default: `https://api.1bluerock.com/v2`) |
| `BLUEROCK_QUEUES` | Comma-separated queue names, or blank to auto-discover |
| `GOOGLE_SHEETS_ID` | Google Sheets spreadsheet ID (from the URL) |
| `GOOGLE_SHEETS_CREDENTIALS_JSON` | Full contents of `credentials.json` as a JSON string (use on Railway) |
| `GOOGLE_SHEETS_CREDENTIALS_FILE` | Path to `credentials.json` (local dev only; ignored if `_JSON` is set) |
| `GOOGLE_SHEETS_TAB` | Deals tab name (default: `Form Responses 1`) |
| `GOOGLE_SHEETS_AGENTS_TAB` | Agents tab name (default: `Active Agents`) |
| `DATABASE_PATH` | SQLite file path (default: `data/callcenter.db`) |
| `BLUEROCK_SYNC_INTERVAL` | BlueRock sync frequency in seconds (default: `30`) |
| `SHEETS_SYNC_INTERVAL` | Sheets sync frequency in seconds (default: `300`) |

Column index variables (`GOOGLE_SHEETS_COL_*`) are documented in `.env.example`.

---

## Deploying to Railway

### Prerequisites

- A [Railway](https://railway.app) account
- Your repo pushed to GitHub

### Steps

1. **Push to GitHub** (see below)
2. In the Railway dashboard, click **New Project → Deploy from GitHub repo** and select your repo
3. Railway will auto-detect Python and run `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Go to your service → **Variables** tab and add every variable from `.env.example`:

   **Critical variables to set:**
   - `BLUEROCK_API_KEY` — your BlueRock token
   - `GOOGLE_SHEETS_ID` — your spreadsheet ID
   - `GOOGLE_SHEETS_CREDENTIALS_JSON` — paste the **entire contents** of `credentials.json` as one line (minify it first — see tip below)

   **Tip — minifying credentials.json for Railway:**
   ```bash
   # Mac/Linux
   cat credentials.json | python3 -m json.tool --compact

   # PowerShell
   Get-Content credentials.json | python -c "import sys,json; print(json.dumps(json.load(sys.stdin)))"
   ```
   Copy the output and paste it as the value of `GOOGLE_SHEETS_CREDENTIALS_JSON`.

5. Click **Deploy**. Railway will install dependencies and start the server.
6. Your app URL will appear in the Railway dashboard under **Settings → Domains**.

### Database: Postgres is required

This app uses **PostgreSQL** (via the `asyncpg` driver). The agent state timeline (timecards), call history, and roster are all persisted in Postgres — they are **not** ephemeral.

On Railway, add a Postgres database to your project:
1. In your Railway project, click **+ New** → **Database** → **PostgreSQL**
2. Railway automatically injects `DATABASE_URL` into your app's environment — no manual config needed

---

## Project Structure

```
main.py                   # FastAPI app entry point
app/
  config.py               # All env var configuration
  api/
    routes.py             # Main dashboard API endpoints
    agents.py             # Agent roster CRUD
    db_viewer.py          # Database inspection endpoints
  models/
    database.py           # SQLite schema & migrations
  services/
    bluerock.py           # BlueRock API sync + agent status polling
    sheets.py             # Google Sheets sync
    scheduler.py          # APScheduler background jobs
static/
  index.html              # Main dashboard
  agents.html             # Agent roster management
  db-viewer.html          # Database inspector
```
