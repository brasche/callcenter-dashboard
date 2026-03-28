import os
from dotenv import load_dotenv

load_dotenv()

# BlueRock REST API (used by the sync service)
BLUEROCK_API_KEY = os.getenv("BLUEROCK_API_KEY", "")
BLUEROCK_API_URL = os.getenv("BLUEROCK_API_URL", "https://api.1bluerock.com/v2")
# Optional: comma-separated list of queues to track; if empty, auto-discovers via /queues/list
BLUEROCK_QUEUES = os.getenv("BLUEROCK_QUEUES", "")

# BlueRock Web Portal (used for call recording access — separate from the REST API)
BLUEROCK_ACCOUNT  = os.getenv("BLUEROCK_ACCOUNT", "")
BLUEROCK_USERNAME = os.getenv("BLUEROCK_USERNAME", "")
BLUEROCK_PASSWORD = os.getenv("BLUEROCK_PASSWORD", "")

# Google Sheets
# On Railway: set GOOGLE_SHEETS_CREDENTIALS_JSON to the full contents of credentials.json
# Locally: set GOOGLE_SHEETS_CREDENTIALS_FILE to the path of the JSON file
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "")
GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "credentials.json")
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "")
GOOGLE_SHEETS_TAB = os.getenv("GOOGLE_SHEETS_TAB", "Form Responses 1")
GOOGLE_SHEETS_AGENTS_TAB = os.getenv("GOOGLE_SHEETS_AGENTS_TAB", "Active Agents")
# Number of header rows to skip (1 = first row is a header)
GOOGLE_SHEETS_HEADER_ROW = int(os.getenv("GOOGLE_SHEETS_HEADER_ROW", "1"))
# Zero-based column indices — mapped to "Form Responses 1"
# A=0 Enrollment Status, B=1 Timestamp, C=2 Client Name, D=3 Backend,
# E=4 Enrolled Date, F=5 Agent Name, G=6 Client ID (phone), H=7 Enrolled Debt Amount
# I=8 First Payment Date, J=9 First Payment Amount
GOOGLE_SHEETS_COL_TIMESTAMP         = int(os.getenv("GOOGLE_SHEETS_COL_TIMESTAMP", "1"))
GOOGLE_SHEETS_COL_AGENT             = int(os.getenv("GOOGLE_SHEETS_COL_AGENT", "5"))
GOOGLE_SHEETS_COL_PHONE             = int(os.getenv("GOOGLE_SHEETS_COL_PHONE", "6"))
GOOGLE_SHEETS_COL_CLOSE_DATE        = int(os.getenv("GOOGLE_SHEETS_COL_CLOSE_DATE", "4"))
GOOGLE_SHEETS_COL_DEAL_VALUE        = int(os.getenv("GOOGLE_SHEETS_COL_DEAL_VALUE", "7"))
GOOGLE_SHEETS_COL_FIRST_PAYMENT     = int(os.getenv("GOOGLE_SHEETS_COL_FIRST_PAYMENT", "9"))
GOOGLE_SHEETS_COL_STATUS            = int(os.getenv("GOOGLE_SHEETS_COL_STATUS", "0"))
GOOGLE_SHEETS_COL_CLIENT_NAME       = int(os.getenv("GOOGLE_SHEETS_COL_CLIENT_NAME", "2"))

# Database
# On Railway: DATABASE_URL is injected automatically when you add a Postgres plugin.
# Locally: set DATABASE_URL in your .env file.
DATABASE_URL  = os.getenv("DATABASE_URL", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/callcenter.db")  # legacy, unused with Postgres

# Sync intervals (seconds)
BLUEROCK_SYNC_INTERVAL = int(os.getenv("BLUEROCK_SYNC_INTERVAL", "30"))
SHEETS_SYNC_INTERVAL = int(os.getenv("SHEETS_SYNC_INTERVAL", "300"))

# Active hours — BlueRock API calls are skipped outside this window to save resources.
# ACTIVE_TIMEZONE: any IANA timezone string, e.g. "America/New_York", "America/Chicago"
# ACTIVE_HOURS_START / END: 24-hour integers (default 7am–8pm)
ACTIVE_TIMEZONE    = os.getenv("ACTIVE_TIMEZONE", "America/New_York")
ACTIVE_HOURS_START = int(os.getenv("ACTIVE_HOURS_START", "7"))
ACTIVE_HOURS_END   = int(os.getenv("ACTIVE_HOURS_END", "20"))
