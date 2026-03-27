"""
BlueRock Communicator API sync service.

Endpoints used (all GET, Bearer token auth):
  /queues/list                          -> list of queue names
  /queue/{name}/calls/today             -> completed calls today per queue
  /agents/stats                         -> per-agent aggregated stats for today
  /agents/status                        -> real-time per-agent status (in_call / available / logged_off)
  /queue/{name}/agentsavailable         -> count of available (not-on-call) agents

Sync strategy (runs every 30 s):
  1. Fetch queue list (or use BLUEROCK_QUEUES from config)
  2. For each queue: fetch today's completed calls -> upsert into `calls`
  3. Fetch all agents stats -> stored as-is in `agent_stats_cache` (in-memory)
  4. Fetch /agents/status -> for agents "in_call", set active_call_since = now - duration_sec
  5. Fetch agentsavailable per queue -> derive a rough "live call count"
"""

import asyncio
import hashlib
import json
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from app.config import (
    BLUEROCK_API_KEY, BLUEROCK_API_URL, BLUEROCK_QUEUES,
    ACTIVE_TIMEZONE, ACTIVE_HOURS_START, ACTIVE_HOURS_END,
)
from app.models.database import get_db


def _is_active_hours() -> bool:
    """Return True if the current local time is within configured active hours."""
    try:
        tz = ZoneInfo(ACTIVE_TIMEZONE)
        hour = datetime.now(tz).hour
        return ACTIVE_HOURS_START <= hour < ACTIVE_HOURS_END
    except Exception:
        return True  # on any error, allow sync

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory cache shared with the API layer
# ---------------------------------------------------------------------------
_agent_stats_cache: list[dict] = []
_queue_stats_cache: list[dict] = []
_last_sync_error: str | None = None
_agent_activity: dict[str, dict] = {}


def get_agent_stats_cache() -> list[dict]:
    return _agent_stats_cache


def get_queue_availability_cache() -> list[dict]:
    return _queue_stats_cache


def get_agent_activity() -> dict:
    return _agent_activity


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    return {"Authorization": f"Bearer {BLUEROCK_API_KEY}"}


async def _get(client: httpx.AsyncClient, path: str) -> dict | None:
    url = f"{BLUEROCK_API_URL.rstrip('/')}/{path.lstrip('/')}"
    try:
        resp = await client.get(url, headers=_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("success")) != "1":
            logger.warning("BlueRock API error for %s: %s", path, data.get("error"))
            return None
        return data.get("response")
    except httpx.HTTPStatusError as exc:
        logger.error("HTTP %s for %s: %s", exc.response.status_code, path, exc)
        return None
    except Exception as exc:
        logger.error("Request failed for %s: %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Call record helpers
# ---------------------------------------------------------------------------

def _make_call_id(queue: str, datetime_str: str, callerid: str, agent: str) -> str:
    raw = f"{queue}|{datetime_str}|{callerid}|{agent}"
    return hashlib.sha1(raw.encode()).hexdigest()


def _parse_call(raw: dict, queue: str) -> dict:
    dt_str = raw.get("datetime", "")
    callerid = raw.get("callerid") or ""
    agent = raw.get("agent") or ""
    calltime = int(raw.get("calltime") or 0)

    direction = "outbound" if "outbound" in queue.lower() else "inbound"

    exit_reason = (raw.get("exit_reason") or "").lower()
    answered = exit_reason in ("agent hang up", "caller hang up")

    return {
        "id":           _make_call_id(queue, dt_str, callerid, agent),
        "agent_id":     agent,
        "agent_name":   agent,
        "phone_number": callerid,
        "direction":    direction,
        "queue":        queue,
        "started_at":   dt_str,
        "ended_at":     dt_str,
        "duration_sec": calltime,
        "hold_sec":     int(raw.get("holdtime") or 0),
        "disposition":  raw.get("disposition"),
        "exit_reason":  raw.get("exit_reason"),
        "answered":     1 if answered else 0,
        "raw_json":     json.dumps(raw),
    }


# ---------------------------------------------------------------------------
# Database upsert
# ---------------------------------------------------------------------------

_UPSERT_CALL = """
INSERT INTO calls (
    id, agent_id, agent_name, phone_number, direction,
    queue, started_at, ended_at, duration_sec, hold_sec,
    answered, exit_reason, disposition, raw_json
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT(id) DO UPDATE SET
    duration_sec = EXCLUDED.duration_sec,
    hold_sec     = EXCLUDED.hold_sec,
    answered     = EXCLUDED.answered,
    exit_reason  = EXCLUDED.exit_reason,
    direction    = EXCLUDED.direction,
    disposition  = EXCLUDED.disposition,
    queue        = EXCLUDED.queue,
    raw_json     = EXCLUDED.raw_json
"""


async def _upsert_calls(calls: list[dict]) -> int:
    if not calls:
        return 0
    async with get_db() as db:
        for c in calls:
            await db.execute(
                _UPSERT_CALL,
                c["id"], c["agent_id"], c["agent_name"], c["phone_number"],
                c["direction"], c.get("queue"), c["started_at"], c["ended_at"],
                c["duration_sec"], c.get("hold_sec", 0), c.get("answered", 0),
                c.get("exit_reason"), c["disposition"], c["raw_json"],
            )
    return len(calls)


_UPSERT_RNA = """
INSERT INTO ring_no_answer_events
    (queue, event_at, callerid, trunk, ringtime, agent,
     enter_position, exit_reason, exit_key, exit_position)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT(queue, event_at, callerid) DO NOTHING
"""


async def _upsert_rna_events(events: list[dict], queue: str) -> int:
    if not events:
        return 0
    async with get_db() as db:
        for e in events:
            await db.execute(
                _UPSERT_RNA,
                queue,
                e.get("datetime") or "",
                e.get("callerid") or "",
                e.get("trunk") or "",
                int(e.get("ringtime") or 0),
                e.get("agent") or "",
                int(e.get("enter_position") or 0),
                e.get("exit_reason") or "",
                e.get("exit_key") or "",
                int(e.get("exit_position") or 0),
            )
    return len(events)


async def _update_sync_log(source: str, records: int, error: str | None = None):
    async with get_db() as db:
        await db.execute(
            """UPDATE sync_log
               SET last_synced_at  = NOW()::TEXT,
                   last_error      = $1,
                   records_written = $2
               WHERE source = $3""",
            error, records, source,
        )


# ---------------------------------------------------------------------------
# Main sync entry point
# ---------------------------------------------------------------------------

async def sync_bluerock():
    """Poll BlueRock and persist results. Called every 30 s by the scheduler."""
    global _agent_stats_cache, _queue_stats_cache, _last_sync_error

    if not BLUEROCK_API_KEY:
        logger.warning("BLUEROCK_API_KEY not set — skipping sync")
        return

    if not _is_active_hours():
        logger.debug("Outside active hours (%s–%s %s) — skipping BlueRock sync",
                     ACTIVE_HOURS_START, ACTIVE_HOURS_END, ACTIVE_TIMEZONE)
        return

    logger.info("BlueRock sync starting…")
    total_written = 0
    error_msg = None

    try:
        async with httpx.AsyncClient() as client:
            # 1. Discover queues
            if BLUEROCK_QUEUES:
                queues = [q.strip() for q in BLUEROCK_QUEUES.split(",") if q.strip()]
            else:
                resp = await _get(client, "/queues/list")
                queues = resp.get("queues", []) if resp else []
                if not queues:
                    logger.warning("BlueRock: no queues returned from /queues/list")

            # 2. Fetch agent stats + track activity
            agents_resp = await _get(client, "/agents/stats")
            if agents_resp:
                raw_agents = agents_resp.get("agents", [])
                _agent_stats_cache = raw_agents
                now_iso = datetime.now(timezone.utc).isoformat()

                for ag in raw_agents:
                    name = ag.get("agent") or ""
                    if not name:
                        continue
                    current_ib      = int(ag.get("inbound_calls") or 0)
                    log_time_sec    = int(ag.get("log_time") or 0)
                    pause_time_sec  = int(ag.get("pause_time") or 0)
                    active_time_sec = max(log_time_sec - pause_time_sec, 0)
                    prev            = _agent_activity.get(name, {})
                    prev_ib         = prev.get("ib_call_count", -1)

                    if current_ib > prev_ib:
                        _agent_activity[name] = {
                            **prev,
                            "ib_call_count":          current_ib,
                            "last_call_completed_at": now_iso,
                            "active_call_since":      now_iso,
                            "log_time_sec":           log_time_sec,
                            "pause_time_sec":         pause_time_sec,
                            "active_time_sec":        active_time_sec,
                        }
                    else:
                        _agent_activity[name] = {
                            **prev,
                            "ib_call_count":   current_ib,
                            "log_time_sec":    log_time_sec,
                            "pause_time_sec":  pause_time_sec,
                            "active_time_sec": active_time_sec,
                        }

                logger.debug("Cached %d agent stat rows", len(_agent_stats_cache))

            # 3. Fetch calls + availability per queue
            availability: list[dict] = []

            async def fetch_queue(queue_name: str):
                nonlocal total_written
                calls_resp = await _get(client, f"/queue/{queue_name}/calls/today")
                if calls_resp:
                    raw_calls = calls_resp.get("calls", [])
                    parsed = [_parse_call(c, queue_name) for c in raw_calls]
                    written = await _upsert_calls(parsed)
                    total_written += written
                    logger.debug("Queue %s: %d calls written", queue_name, written)

                    if "outbound" not in queue_name.lower():
                        for c in parsed:
                            agent = c["agent_name"]
                            dur   = c["duration_sec"]
                            if not agent or not dur:
                                continue
                            try:
                                start = datetime.strptime(c["started_at"], "%Y-%m-%d %H:%M:%S")
                                end   = start + timedelta(seconds=dur)
                                end_iso = end.isoformat()
                                prev = _agent_activity.get(agent, {})
                                prev_since = prev.get("active_call_since", "")
                                if not prev_since or end_iso > prev_since:
                                    _agent_activity[agent] = {
                                        **prev,
                                        "active_call_since": end_iso,
                                    }
                            except Exception:
                                pass

                rna_resp = await _get(client, f"/queue/{queue_name}/ringnoanswer/today")
                if rna_resp:
                    raw_rna = (rna_resp.get("ringnoanswer")
                               or rna_resp.get("ring_no_answer")
                               or rna_resp.get("calls")
                               or [])
                    await _upsert_rna_events(raw_rna, queue_name)
                    logger.debug("Queue %s: %d RNA events upserted", queue_name, len(raw_rna))

                avail_resp = await _get(client, f"/queue/{queue_name}/agentsavailable")
                if avail_resp is not None:
                    availability.append({
                        "queue":     queue_name,
                        "available": avail_resp.get("available", 0),
                    })

            await asyncio.gather(*[fetch_queue(q) for q in queues])
            _queue_stats_cache = availability

    except Exception as exc:
        error_msg = str(exc)
        _last_sync_error = error_msg
        logger.exception("BlueRock sync failed: %s", exc)

    await _update_sync_log("bluerock", total_written, error_msg)
    logger.info("BlueRock sync done — %d records written", total_written)


# ---------------------------------------------------------------------------
# Agent status polling + state-block stitching
# ---------------------------------------------------------------------------

async def _stitch_blocks(db, agents_snapshot: dict, polled_at: str) -> None:
    """Recompute agent_state_blocks from the latest /agents/status snapshot."""
    try:
        now_dt = datetime.fromisoformat(polled_at)
    except Exception:
        now_dt = datetime.now(timezone.utc)

    for agent_name, state_data in agents_snapshot.items():
        status = (state_data.get("status") or "unknown").lower()
        duration_in_state = int(state_data.get("duration") or 0)

        state_started_dt = now_dt - timedelta(seconds=duration_in_state)
        state_started = state_started_dt.isoformat()

        try:
            last = await db.fetchrow(
                """SELECT id, state, started_at, ended_at
                   FROM agent_state_blocks
                   WHERE agent_name = $1
                   ORDER BY started_at DESC LIMIT 1""",
                agent_name,
            )

            if last and last["state"] == status and last["ended_at"] is None:
                # Same state, block still open — update duration only, keep ended_at NULL
                block_start_dt = datetime.fromisoformat(last["started_at"])
                dur = max(int((now_dt - block_start_dt).total_seconds()), 0)
                await db.execute(
                    "UPDATE agent_state_blocks SET duration_seconds = $1 WHERE id = $2",
                    dur, last["id"],
                )
            else:
                # State changed — close the previous open block
                if last and last["ended_at"] is None:
                    block_start_dt = datetime.fromisoformat(last["started_at"])
                    dur = max(int((state_started_dt - block_start_dt).total_seconds()), 0)
                    await db.execute(
                        "UPDATE agent_state_blocks SET ended_at = $1, duration_seconds = $2 WHERE id = $3",
                        state_started, dur, last["id"],
                    )
                # Open a new block for the new state
                await db.execute(
                    """INSERT INTO agent_state_blocks
                       (agent_name, state, started_at, ended_at, duration_seconds)
                       VALUES ($1, $2, $3, NULL, $4)
                       ON CONFLICT (agent_name, started_at) DO NOTHING""",
                    agent_name, status, state_started, duration_in_state,
                )
        except Exception as exc:
            logger.error("_stitch_blocks error for %s: %s", agent_name, exc)


async def poll_agent_status() -> None:
    """Poll /agents/status every 30 s, write raw snapshots, stitch state blocks."""
    if not BLUEROCK_API_KEY:
        return

    if not _is_active_hours():
        return

    logger.debug("Polling /agents/status…")
    try:
        async with httpx.AsyncClient() as client:
            resp = await _get(client, "/agents/status")
        if not resp:
            return

        agents = resp.get("agents", {})
        if not agents:
            return

        polled_at = datetime.now(timezone.utc).isoformat()
        async with get_db() as db:
            for agent_name, state_data in agents.items():
                await db.execute(
                    """INSERT INTO agent_status_snapshots
                       (agent_name, polled_at, status, duration_in_state, callerid_num, queue)
                       VALUES ($1, $2, $3, $4, $5, $6)""",
                    agent_name,
                    polled_at,
                    (state_data.get("status") or "").lower(),
                    int(state_data.get("duration") or 0),
                    state_data.get("callerid_num") or "",
                    state_data.get("queue") or "",
                )

            await _stitch_blocks(db, agents, polled_at)
            logger.debug("Status poll done: %d agents", len(agents))

    except Exception as exc:
        logger.error("poll_agent_status failed: %s", exc)


async def sync_agent_daily_stats() -> None:
    """Poll /agents/stats every 5 min and upsert into agent_daily_stats."""
    if not BLUEROCK_API_KEY:
        return

    logger.debug("Syncing agent_daily_stats…")
    try:
        async with httpx.AsyncClient() as client:
            resp = await _get(client, "/agents/stats")
        if not resp:
            return

        raw_agents = resp.get("agents", [])
        today = date.today().isoformat()

        async with get_db() as db:
            for ag in raw_agents:
                name = ag.get("agent") or ""
                if not name:
                    continue
                await db.execute(
                    """INSERT INTO agent_daily_stats
                       (agent_name, date, log_time, pause_time, wrapup_time,
                        idle_time, inbound_calls, outbound_calls)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                       ON CONFLICT(agent_name, date) DO UPDATE SET
                         log_time       = EXCLUDED.log_time,
                         pause_time     = EXCLUDED.pause_time,
                         wrapup_time    = EXCLUDED.wrapup_time,
                         idle_time      = EXCLUDED.idle_time,
                         inbound_calls  = EXCLUDED.inbound_calls,
                         outbound_calls = EXCLUDED.outbound_calls,
                         synced_at      = NOW()::TEXT""",
                    name, today,
                    int(ag.get("log_time")       or 0),
                    int(ag.get("pause_time")     or 0),
                    int(ag.get("wrapup_time")    or 0),
                    int(ag.get("idle_time")      or 0),
                    int(ag.get("inbound_calls")  or 0),
                    int(ag.get("outbound_calls") or 0),
                )
            logger.debug("agent_daily_stats: %d rows upserted for %s", len(raw_agents), today)

    except Exception as exc:
        logger.error("sync_agent_daily_stats failed: %s", exc)


async def fetch_agent_activity(agent_name: str) -> dict:
    """Fetch every available BlueRock field for a single agent."""
    if not BLUEROCK_API_KEY:
        return {}

    candidates = [
        f"/agents/{agent_name}/activity",
        f"/agents/{agent_name}/pauses",
        f"/agents/{agent_name}/loginlog",
        f"/agents/{agent_name}/history",
        f"/agents/{agent_name}/log",
    ]

    results = {}
    async with httpx.AsyncClient() as client:
        for path in candidates:
            data = await _get(client, path)
            if data is not None:
                results[path] = data

    return results
