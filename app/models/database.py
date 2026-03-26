from contextlib import asynccontextmanager
import asyncpg
from app.config import DATABASE_URL


_pool: asyncpg.Pool | None = None


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        url = DATABASE_URL
        if not url:
            raise RuntimeError(
                "DATABASE_URL is not set. "
                "On Railway: add a PostgreSQL plugin to your project — it injects DATABASE_URL automatically. "
                "Locally: add DATABASE_URL=postgresql://... to your .env file."
            )
        # Railway injects postgres:// — asyncpg needs postgresql://
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if "localhost" in url or "127.0.0.1" in url:
            raise RuntimeError(
                f"DATABASE_URL points to localhost ({url}). "
                "On Railway this won't work — delete the DATABASE_URL variable and let the "
                "PostgreSQL plugin inject the real one automatically."
            )
        _pool = await asyncpg.create_pool(url, min_size=1, max_size=10)
    return _pool


@asynccontextmanager
async def get_db():
    pool = await _get_pool()
    async with pool.acquire() as conn:
        yield conn


async def init_db():
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for stmt in _split_sql(SCHEMA_SQL):
                await conn.execute(stmt)
    await _run_migrations()


def _split_sql(sql: str) -> list[str]:
    return [s.strip() for s in sql.split(";") if s.strip()]


# ── Idempotent migrations (safe to run on every startup) ────────────────────
# In Postgres, ALTER TABLE ADD COLUMN IF NOT EXISTS won't error if column exists.
_MIGRATIONS = [
    "ALTER TABLE agents ADD COLUMN IF NOT EXISTS bluerock_username TEXT",
    "ALTER TABLE agents ADD COLUMN IF NOT EXISTS active INTEGER DEFAULT 1",
    "ALTER TABLE deals  ADD COLUMN IF NOT EXISTS first_payment_amount REAL",
    "ALTER TABLE deals  ADD COLUMN IF NOT EXISTS submitted_at TEXT",
    """CREATE TABLE IF NOT EXISTS agent_status_snapshots (
        id                SERIAL PRIMARY KEY,
        agent_name        TEXT NOT NULL,
        polled_at         TEXT NOT NULL,
        status            TEXT,
        duration_in_state INTEGER,
        callerid_num      TEXT,
        queue             TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_agent_at ON agent_status_snapshots(agent_name, polled_at)",
    """CREATE TABLE IF NOT EXISTS agent_state_blocks (
        id               SERIAL PRIMARY KEY,
        agent_name       TEXT NOT NULL,
        state            TEXT NOT NULL,
        started_at       TEXT NOT NULL,
        ended_at         TEXT,
        duration_seconds INTEGER
    )""",
    "CREATE INDEX IF NOT EXISTS idx_blocks_agent_date ON agent_state_blocks(agent_name, started_at)",
    # Remove duplicate blocks first, then add unique constraint (safe to re-run — no-op when clean)
    "DELETE FROM agent_state_blocks WHERE id NOT IN (SELECT MAX(id) FROM agent_state_blocks GROUP BY agent_name, started_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_agent_started_unique ON agent_state_blocks(agent_name, started_at)",
    """CREATE TABLE IF NOT EXISTS agent_daily_stats (
        id             SERIAL PRIMARY KEY,
        agent_name     TEXT NOT NULL,
        date           TEXT NOT NULL,
        log_time       INTEGER DEFAULT 0,
        pause_time     INTEGER DEFAULT 0,
        wrapup_time    INTEGER DEFAULT 0,
        idle_time      INTEGER DEFAULT 0,
        inbound_calls  INTEGER DEFAULT 0,
        outbound_calls INTEGER DEFAULT 0,
        synced_at      TEXT DEFAULT NOW()::TEXT,
        UNIQUE(agent_name, date)
    )""",
]


async def _run_migrations():
    pool = await _get_pool()
    async with pool.acquire() as conn:
        for sql in _MIGRATIONS:
            try:
                await conn.execute(sql)
            except Exception:
                pass  # already exists / idempotent


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS calls (
    id              TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    agent_name      TEXT NOT NULL,
    phone_number    TEXT,
    direction       TEXT,
    queue           TEXT,
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    duration_sec    INTEGER,
    hold_sec        INTEGER,
    answered        INTEGER DEFAULT 1,
    disposition     TEXT,
    exit_reason     TEXT,
    raw_json        TEXT,
    created_at      TEXT DEFAULT NOW()::TEXT
);

CREATE INDEX IF NOT EXISTS idx_calls_agent_id   ON calls(agent_id);
CREATE INDEX IF NOT EXISTS idx_calls_started_at ON calls(started_at);
CREATE INDEX IF NOT EXISTS idx_calls_phone      ON calls(phone_number);
CREATE INDEX IF NOT EXISTS idx_calls_queue      ON calls(queue);

CREATE TABLE IF NOT EXISTS live_calls (
    id              TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    agent_name      TEXT NOT NULL,
    phone_number    TEXT,
    direction       TEXT,
    started_at      TEXT NOT NULL,
    duration_sec    INTEGER,
    raw_json        TEXT,
    synced_at       TEXT DEFAULT NOW()::TEXT
);

CREATE TABLE IF NOT EXISTS deals (
    id                SERIAL PRIMARY KEY,
    agent_name        TEXT,
    client_name       TEXT,
    phone_number      TEXT,
    enrollment_status TEXT,
    closed_at         TEXT,
    deal_value        REAL,
    first_payment_amount REAL,
    submitted_at      TEXT,
    raw_row           TEXT,
    sheet_row         INTEGER,
    synced_at         TEXT DEFAULT NOW()::TEXT
);

CREATE INDEX IF NOT EXISTS idx_deals_phone     ON deals(phone_number);
CREATE INDEX IF NOT EXISTS idx_deals_agent     ON deals(agent_name);
CREATE INDEX IF NOT EXISTS idx_deals_closed_at ON deals(closed_at);

CREATE TABLE IF NOT EXISTS agents (
    id                SERIAL PRIMARY KEY,
    short_name        TEXT UNIQUE,
    full_name         TEXT,
    email             TEXT,
    bluerock_username TEXT,
    active            INTEGER DEFAULT 1,
    synced_at         TEXT DEFAULT NOW()::TEXT
);

CREATE TABLE IF NOT EXISTS sync_log (
    source          TEXT PRIMARY KEY,
    last_synced_at  TEXT,
    last_error      TEXT,
    records_written INTEGER DEFAULT 0
);

INSERT INTO sync_log (source) VALUES ('bluerock')       ON CONFLICT DO NOTHING;
INSERT INTO sync_log (source) VALUES ('google_sheets')  ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS agent_status_snapshots (
    id                SERIAL PRIMARY KEY,
    agent_name        TEXT NOT NULL,
    polled_at         TEXT NOT NULL,
    status            TEXT,
    duration_in_state INTEGER,
    callerid_num      TEXT,
    queue             TEXT
);

CREATE INDEX IF NOT EXISTS idx_snapshots_agent_at ON agent_status_snapshots(agent_name, polled_at);

CREATE TABLE IF NOT EXISTS agent_state_blocks (
    id               SERIAL PRIMARY KEY,
    agent_name       TEXT NOT NULL,
    state            TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    ended_at         TEXT,
    duration_seconds INTEGER
);

CREATE INDEX IF NOT EXISTS idx_blocks_agent_date ON agent_state_blocks(agent_name, started_at);

CREATE TABLE IF NOT EXISTS agent_daily_stats (
    id             SERIAL PRIMARY KEY,
    agent_name     TEXT NOT NULL,
    date           TEXT NOT NULL,
    log_time       INTEGER DEFAULT 0,
    pause_time     INTEGER DEFAULT 0,
    wrapup_time    INTEGER DEFAULT 0,
    idle_time      INTEGER DEFAULT 0,
    inbound_calls  INTEGER DEFAULT 0,
    outbound_calls INTEGER DEFAULT 0,
    synced_at      TEXT DEFAULT NOW()::TEXT,
    UNIQUE(agent_name, date)
)
"""
