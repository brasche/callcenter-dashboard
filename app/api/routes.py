"""
FastAPI routes for the call center dashboard.

Endpoints:
  GET /api/status              -- sync health
  GET /api/agents              -- per-agent metrics (today)
  GET /api/agents/{name}       -- agent drill-down: per-queue breakdown + recent calls
  GET /api/team                -- team-total summary
  GET /api/live                -- available-agent counts per queue
"""

import json
from datetime import date, datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.config import BLUEROCK_API_KEY, BLUEROCK_API_URL
from app.models.database import get_db
from app.services.bluerock import (
    get_agent_stats_cache,
    get_queue_availability_cache,
    get_agent_activity,
    fetch_agent_activity,
)

router = APIRouter()


def _today_range():
    today = date.today().isoformat()
    return f"{today} 00:00:00", f"{today} 23:59:59"


def _date_range(start: Optional[str], end: Optional[str]):
    if start:
        start_dt = f"{start} 00:00:00"
    else:
        start_dt = f"{date.today().isoformat()} 00:00:00"
    if end:
        end_dt = f"{end} 23:59:59"
    else:
        end_dt = f"{date.today().isoformat()} 23:59:59"
    return start_dt, end_dt


def _is_subseq(needle: str, haystack: str) -> bool:
    i = 0
    for ch in haystack:
        if i < len(needle) and ch == needle[i]:
            i += 1
    return i == len(needle)


def _auto_match(br_username: str, short_to_full: dict) -> str | None:
    if len(br_username) < 2:
        return None
    br = br_username.lower()
    last_init = br[-1]
    first_part = br[:-1]
    for short in short_to_full:
        parts = short.split()
        if not parts:
            continue
        first = parts[0].lower()
        li = parts[-1][0].lower() if len(parts) > 1 else ""
        if li == last_init and _is_subseq(first_part, first):
            return short_to_full[short]
    return None


def _pct(part, total):
    return round(part / total * 100, 1) if total else 0


def _row_to_dict(row) -> dict:
    return dict(row)


# ---------------------------------------------------------------------------
# /api/status
# ---------------------------------------------------------------------------

@router.get("/status")
async def get_status():
    async with get_db() as db:
        rows = await db.fetch("SELECT * FROM sync_log")
    return {"sync": [_row_to_dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# /api/debug/agent-stats
# ---------------------------------------------------------------------------

@router.get("/debug/agent-stats")
async def get_debug_agent_stats():
    return {
        "raw":      get_agent_stats_cache(),
        "activity": get_agent_activity(),
    }


# ---------------------------------------------------------------------------
# /api/live
# ---------------------------------------------------------------------------

@router.get("/live")
async def get_live():
    availability = get_queue_availability_cache()
    return {
        "queues": [
            {"queue": q["queue"], "agents_available": int(q.get("available", 0))}
            for q in availability
        ],
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# /api/agents
# ---------------------------------------------------------------------------

@router.get("/agents")
async def get_agents(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    start_dt, end_dt = _date_range(start, end)
    is_today = (start is None and end is None) or (
        start == date.today().isoformat() and (end is None or end == date.today().isoformat())
    )
    async with get_db() as db:
        # ── IB call metrics per agent ────────────────────────────────────
        agent_rows = [_row_to_dict(r) for r in await db.fetch(
            """
            SELECT
                c.agent_name,
                a.full_name,
                COUNT(*)                                              AS total_calls,
                SUM(c.duration_sec)                                   AS total_talk_sec,
                CAST(AVG(c.duration_sec) AS INTEGER)                  AS avg_talk_sec,
                MAX(c.duration_sec)                                   AS max_talk_sec,
                SUM(CASE WHEN c.duration_sec >= 120  THEN 1 ELSE 0 END) AS calls_2min,
                SUM(CASE WHEN c.duration_sec >= 300  THEN 1 ELSE 0 END) AS calls_5min,
                SUM(CASE WHEN c.duration_sec >= 600  THEN 1 ELSE 0 END) AS calls_10min,
                SUM(CASE WHEN c.duration_sec >= 1200 THEN 1 ELSE 0 END) AS calls_20min,
                MIN(c.started_at) AS first_call,
                MAX(c.started_at) AS last_call
            FROM calls c
            LEFT JOIN agents a ON LOWER(a.bluerock_username) = LOWER(c.agent_name)
            WHERE c.started_at BETWEEN $1 AND $2
              AND c.answered = 1
              AND c.direction = 'inbound'
            GROUP BY c.agent_name, a.full_name
            ORDER BY total_calls DESC
            """,
            start_dt, end_dt,
        )]

        # ── OB call counts ────────────────────────────────────────────────
        ob_counts = {
            r["agent_name"]: r["ob_calls"]
            for r in await db.fetch(
                """
                SELECT c.agent_name, COUNT(*) AS ob_calls
                FROM calls c
                WHERE c.started_at BETWEEN $1 AND $2
                  AND c.answered = 1
                  AND c.direction = 'outbound'
                GROUP BY c.agent_name
                """,
                start_dt, end_dt,
            )
        }

        # ── short_name -> full_name (run BEFORE deals query) ─────────────
        short_to_full: dict[str, str] = {
            r["short_name"]: r["full_name"]
            for r in await db.fetch(
                "SELECT short_name, full_name FROM agents WHERE full_name IS NOT NULL"
            )
        }

        # Persist auto-matched bluerock_username back so the deals JOIN works
        for ag_row in agent_rows:
            br = ag_row["agent_name"]
            if ag_row["full_name"]:
                continue
            matched_full = _auto_match(br, short_to_full)
            if matched_full:
                short = next((s for s, f in short_to_full.items() if f == matched_full), None)
                if short:
                    await db.execute(
                        """UPDATE agents SET bluerock_username = $1
                           WHERE short_name = $2
                             AND (bluerock_username IS NULL OR bluerock_username = '')""",
                        br, short,
                    )

        # ── Deals per agent ───────────────────────────────────────────────
        deal_rows_raw = await db.fetch(
            """
            SELECT a.bluerock_username,
                   a.short_name,
                   COUNT(DISTINCT d.id)        AS total_deals,
                   AVG(d.deal_value)           AS avg_debt,
                   AVG(d.first_payment_amount) AS avg_first_payment,
                   SUM(d.deal_value)           AS total_deal_value
            FROM agents a
            INNER JOIN deals d ON (
                LOWER(d.agent_name) = LOWER(a.short_name)
                OR (a.full_name IS NOT NULL AND LOWER(d.agent_name) = LOWER(a.full_name))
            )
            GROUP BY a.short_name, a.bluerock_username
            """,
        )

        deal_by_agent: dict[str, dict] = {
            str(r["bluerock_username"] or "").lower(): _row_to_dict(r)
            for r in deal_rows_raw if r["bluerock_username"]
        }
        deal_by_short: dict[str, dict] = {
            str(r["short_name"] or "").lower(): _row_to_dict(r)
            for r in deal_rows_raw if r["short_name"]
        }

        # ── Phone→deal fallback ───────────────────────────────────────────
        phone_fullname: dict[str, str] = {
            r["br_name"].lower(): r["full_name"]
            for r in await db.fetch(
                """
                SELECT DISTINCT c.agent_name AS br_name, a.full_name
                FROM calls c
                INNER JOIN deals d ON d.phone_number = c.phone_number
                INNER JOIN agents a ON LOWER(a.short_name) = LOWER(d.agent_name)
                WHERE c.started_at BETWEEN $1 AND $2 AND c.answered = 1
                """,
                start_dt, end_dt,
            )
            if r["full_name"]
        }

        # ── Activity cache ────────────────────────────────────────────────
        activity = get_agent_activity()

        # ── Ring no answer counts per agent ──────────────────────────────
        rna_counts: dict[str, int] = {
            r["agent"]: r["rna_count"]
            for r in await db.fetch(
                """
                SELECT agent, COUNT(*) AS rna_count
                FROM ring_no_answer_events
                WHERE event_at BETWEEN $1 AND $2
                  AND agent IS NOT NULL AND agent != ''
                GROUP BY agent
                """,
                start_dt, end_dt,
            )
        }
        # Merge stats cache RNA as fallback for agents not yet in DB
        for ag in get_agent_stats_cache():
            name = ag.get("agent") or ""
            if name and name not in rna_counts:
                rna_counts[name] = int(ag.get("ring_no_answer") or 0)

        # ── Avg time between IB calls ─────────────────────────────────────
        between_rows = {
            r["agent_name"]: dict(r)
            for r in await db.fetch(
                """
                SELECT agent_name,
                       COUNT(*) AS cnt,
                       MIN(started_at) AS first_call_ts,
                       MAX(started_at) AS last_call_ts
                FROM calls
                WHERE started_at BETWEEN $1 AND $2 AND answered = 1 AND direction = 'inbound'
                GROUP BY agent_name
                HAVING COUNT(*) > 1
                """,
                start_dt, end_dt,
            )
        }

    # ── Assemble ──────────────────────────────────────────────────────────
    agents_out = []
    for ag in agent_rows:
        br_name   = ag["agent_name"]
        full_name = (ag["full_name"]
                     or phone_fullname.get(br_name.lower())
                     or _auto_match(br_name, short_to_full)
                     or br_name)

        total = ag["total_calls"] or 0
        c2    = ag["calls_2min"]  or 0
        c5    = ag["calls_5min"]  or 0
        c10   = ag["calls_10min"] or 0
        c20   = ag["calls_20min"] or 0

        act = activity.get(br_name, {})
        log_time_sec           = act.get("log_time_sec", 0)
        pause_time_sec         = act.get("pause_time_sec", 0)
        active_time_sec        = act.get("active_time_sec", 0)
        last_call_completed_at = act.get("last_call_completed_at")

        if is_today and active_time_sec and active_time_sec > 0:
            active_hours = active_time_sec / 3600
        elif log_time_sec and log_time_sec > 0:
            active_hours = log_time_sec / 3600
        else:
            if ag["first_call"] and ag["last_call"] and ag["first_call"] != ag["last_call"]:
                try:
                    dt_fmt = "%Y-%m-%d %H:%M:%S"
                    span = (datetime.strptime(ag["last_call"], dt_fmt) -
                            datetime.strptime(ag["first_call"], dt_fmt)).total_seconds()
                    active_hours = max(span / 3600, 1/60)
                except Exception:
                    active_hours = 1
            else:
                active_hours = 1

        calls_per_hour = round(total / active_hours, 1) if active_hours else 0

        brow = between_rows.get(br_name)
        avg_between_sec = None
        if brow and brow["cnt"] > 1:
            try:
                dt_fmt = "%Y-%m-%d %H:%M:%S"
                span = (datetime.strptime(brow["last_call_ts"], dt_fmt) -
                        datetime.strptime(brow["first_call_ts"], dt_fmt)).total_seconds()
                avg_between_sec = round(span / (brow["cnt"] - 1))
            except Exception:
                avg_between_sec = None

        agents_out.append({
            "agent_name":             br_name,
            "full_name":              full_name,
            "total_calls":            total,
            "ob_calls":               ob_counts.get(br_name, 0),
            "avg_talk_sec":           ag["avg_talk_sec"] or 0,
            "max_talk_sec":           ag["max_talk_sec"] or 0,
            "total_talk_sec":         ag["total_talk_sec"] or 0,
            "pct_2min":               _pct(c2,  total),
            "pct_5min":               _pct(c5,  total),
            "pct_10min":              _pct(c10, total),
            "pct_20min":              _pct(c20, total),
            "calls_2min":             c2,
            "calls_5min":             c5,
            "calls_10min":            c10,
            "calls_20min":            c20,
            "close_pct_20min":        0,
            "total_deals":            0,
            "avg_debt_enrolled":      0,
            "avg_first_payment":      0,
            "total_deal_value":       0,
            "calls_per_hour":         calls_per_hour,
            "log_time_sec":           log_time_sec,
            "pause_time_sec":         pause_time_sec,
            "active_time_sec":        active_time_sec,
            "active_hours":           round(active_hours, 2),
            "avg_between_calls_sec":  avg_between_sec,
            "last_call_completed_at": last_call_completed_at,
            "first_call":             ag["first_call"],
            "last_call":              ag["last_call"],
            "cps":                    0,
            "ring_no_answer":         rna_counts.get(br_name, 0),
        })

    # ── Stitch deal data ──────────────────────────────────────────────────
    for ag in agents_out:
        br_name = ag["agent_name"]
        d = deal_by_agent.get(br_name.lower(), {})
        if not d:
            matched_full = _auto_match(br_name, short_to_full)
            short = next((s for s, f in short_to_full.items() if f == matched_full), None) if matched_full else None
            if short:
                d = deal_by_short.get(short.lower(), {})
        ag["total_deals"]       = d.get("total_deals", 0) or 0
        ag["avg_debt_enrolled"] = round(d.get("avg_debt") or 0, 2)
        ag["avg_first_payment"] = round(d.get("avg_first_payment") or 0, 2)
        ag["total_deal_value"]  = round(d.get("total_deal_value") or 0, 2)
        ag["close_pct_20min"]   = _pct(ag["total_deals"], ag["calls_20min"])
        deals = ag["total_deals"]
        ag["cps"] = round(ag["total_calls"] / deals, 1) if deals else None

    return {"agents": agents_out, "as_of": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# /api/agents/{name}  — drill-down
# ---------------------------------------------------------------------------

@router.get("/agents/{agent_name}")
async def get_agent_detail(
    agent_name: str,
    start: Optional[str] = Query(None),
    end:   Optional[str] = Query(None),
):
    start_dt, end_dt = _date_range(start, end)
    date_str = (start or date.today().isoformat())

    async with get_db() as db:
        # Per-queue breakdown
        queue_rows = await db.fetch(
            """
            SELECT
                queue,
                COUNT(*)                                              AS total_calls,
                CAST(AVG(duration_sec) AS INTEGER)                    AS avg_talk_sec,
                MAX(duration_sec)                                     AS max_talk_sec,
                SUM(CASE WHEN duration_sec >= 120  THEN 1 ELSE 0 END) AS calls_2min,
                SUM(CASE WHEN duration_sec >= 300  THEN 1 ELSE 0 END) AS calls_5min,
                SUM(CASE WHEN duration_sec >= 600  THEN 1 ELSE 0 END) AS calls_10min,
                SUM(CASE WHEN duration_sec >= 1200 THEN 1 ELSE 0 END) AS calls_20min
            FROM calls
            WHERE agent_name = $1 AND started_at BETWEEN $2 AND $3 AND answered = 1
            GROUP BY queue
            ORDER BY total_calls DESC
            """,
            agent_name, start_dt, end_dt,
        )
        queues = []
        for r in queue_rows:
            d = _row_to_dict(r)
            t = d["total_calls"] or 0
            queues.append({
                **d,
                "pct_2min":  _pct(d["calls_2min"],  t),
                "pct_5min":  _pct(d["calls_5min"],  t),
                "pct_10min": _pct(d["calls_10min"], t),
                "pct_20min": _pct(d["calls_20min"], t),
            })

        # All calls for this date range
        all_calls_asc = [_row_to_dict(r) for r in await db.fetch(
            """
            SELECT id, started_at, queue, direction, phone_number, duration_sec,
                   hold_sec, exit_reason, disposition, answered
            FROM calls
            WHERE agent_name = $1 AND started_at BETWEEN $2 AND $3
            ORDER BY started_at ASC
            """,
            agent_name, start_dt, end_dt,
        )]
        recent_calls = list(reversed(all_calls_asc))

        # Deals for this agent
        deals = [_row_to_dict(r) for r in await db.fetch(
            """
            SELECT DISTINCT d.client_name,
                   d.phone_number AS client_id,
                   d.enrollment_status,
                   d.closed_at, d.deal_value, d.first_payment_amount, d.submitted_at
            FROM deals d
            INNER JOIN agents a ON (
                LOWER(d.agent_name) = LOWER(a.short_name)
                OR (a.full_name IS NOT NULL AND LOWER(d.agent_name) = LOWER(a.full_name))
            )
            WHERE LOWER(a.bluerock_username) = LOWER($1)
            ORDER BY d.submitted_at DESC
            """,
            agent_name,
        )]

        # Agent mapping info
        agent_info_row = await db.fetchrow(
            """SELECT short_name, full_name, bluerock_username FROM agents
               WHERE LOWER(TRIM(bluerock_username)) = LOWER(TRIM($1))
                  OR LOWER(TRIM(short_name)) = LOWER(TRIM($2))
               LIMIT 1""",
            agent_name, agent_name,
        )

        if agent_info_row:
            agent_info = _row_to_dict(agent_info_row)
        else:
            s2f = {
                r["short_name"]: r["full_name"]
                for r in await db.fetch(
                    "SELECT short_name, full_name FROM agents WHERE full_name IS NOT NULL"
                )
            }
            matched_full = _auto_match(agent_name, s2f)
            short = next((s for s, f in s2f.items() if f == matched_full), None) if matched_full else None
            if short:
                await db.execute(
                    """UPDATE agents SET bluerock_username = $1
                       WHERE short_name = $2
                         AND (bluerock_username IS NULL OR bluerock_username = '')""",
                    agent_name, short,
                )
            agent_info = {
                "bluerock_username": agent_name,
                "short_name":        short,
                "full_name":         matched_full or agent_name,
            }

        # State blocks for the requested date
        state_blocks = [_row_to_dict(r) for r in await db.fetch(
            """SELECT state, started_at, ended_at, duration_seconds
               FROM agent_state_blocks
               WHERE agent_name = $1 AND started_at LIKE $2 || '%'
               ORDER BY started_at ASC""",
            agent_name, date_str,
        )]

        # Ring no answer events for this agent on this date
        rna_events = [_row_to_dict(r) for r in await db.fetch(
            """
            SELECT queue, event_at, callerid, ringtime, exit_reason
            FROM ring_no_answer_events
            WHERE agent = $1 AND event_at LIKE $2 || '%'
            ORDER BY event_at ASC
            """,
            agent_name, date_str,
        )]

        # Daily stats
        daily_stats_row = await db.fetchrow(
            """SELECT log_time, pause_time, wrapup_time, idle_time,
                      inbound_calls, outbound_calls, synced_at
               FROM agent_daily_stats
               WHERE agent_name = $1 AND date = $2""",
            agent_name, date_str,
        )
        daily_stats = _row_to_dict(daily_stats_row) if daily_stats_row else {}

    # Raw BlueRock stats
    stats_cache = get_agent_stats_cache()
    br_stats_raw = next(
        (ag for ag in stats_cache if (ag.get("agent") or "").lower() == agent_name.lower()),
        {}
    )
    act = get_agent_activity().get(agent_name, {})
    if act and not br_stats_raw:
        br_stats_raw = {
            "agent":         agent_name,
            "log_time":      act.get("log_time_sec", 0),
            "pause_time":    act.get("pause_time_sec", 0),
            "inbound_calls": act.get("ib_call_count", 0),
        }

    br_activity = await fetch_agent_activity(agent_name)

    return {
        "agent_name":             agent_name,
        "agent_info":             agent_info,
        "log_time_sec":           act.get("log_time_sec", 0),
        "pause_time_sec":         act.get("pause_time_sec", 0),
        "active_time_sec":        act.get("active_time_sec", 0),
        "last_call_completed_at": act.get("last_call_completed_at"),
        "br_stats":               br_stats_raw,
        "br_activity":            br_activity,
        "queues":                 queues,
        "timeline":               all_calls_asc,
        "recent_calls":           recent_calls,
        "deals":                  deals,
        "state_blocks":           state_blocks,
        "daily_stats":            daily_stats,
        "rna_events":             rna_events,
    }


# ---------------------------------------------------------------------------
# /api/team
# ---------------------------------------------------------------------------

@router.get("/team")
async def get_team(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    start_dt, end_dt = _date_range(start, end)
    async with get_db() as db:
        s = _row_to_dict(await db.fetchrow(
            """
            SELECT
                COUNT(*)                                              AS total_calls,
                SUM(duration_sec)                                     AS total_talk_sec,
                CAST(AVG(duration_sec) AS INTEGER)                    AS avg_talk_sec,
                MAX(duration_sec)                                     AS max_talk_sec,
                SUM(CASE WHEN duration_sec >= 120  THEN 1 ELSE 0 END) AS calls_2min,
                SUM(CASE WHEN duration_sec >= 300  THEN 1 ELSE 0 END) AS calls_5min,
                SUM(CASE WHEN duration_sec >= 600  THEN 1 ELSE 0 END) AS calls_10min,
                SUM(CASE WHEN duration_sec >= 1200 THEN 1 ELSE 0 END) AS calls_20min
            FROM calls
            WHERE started_at BETWEEN $1 AND $2 AND answered = 1 AND direction = 'inbound'
            """,
            start_dt, end_dt,
        ))

        ds = _row_to_dict(await db.fetchrow(
            """
            SELECT COUNT(*) AS total_deals,
                   AVG(deal_value) AS avg_debt,
                   AVG(first_payment_amount) AS avg_first_payment,
                   SUM(deal_value) AS total_value
            FROM deals WHERE submitted_at IS NOT NULL
            """,
        ))

    total       = s["total_calls"] or 0
    c20         = s["calls_20min"] or 0
    total_deals = ds["total_deals"] or 0

    return {
        "total_calls":       total,
        "avg_talk_sec":      s["avg_talk_sec"] or 0,
        "max_talk_sec":      s["max_talk_sec"] or 0,
        "total_talk_sec":    s["total_talk_sec"] or 0,
        "pct_2min":          _pct(s["calls_2min"]  or 0, total),
        "pct_5min":          _pct(s["calls_5min"]  or 0, total),
        "pct_10min":         _pct(s["calls_10min"] or 0, total),
        "pct_20min":         _pct(c20, total),
        "close_pct_20min":   _pct(total_deals, c20),
        "total_deals":       total_deals,
        "avg_debt_enrolled": round(ds["avg_debt"] or 0, 2),
        "avg_first_payment": round(ds["avg_first_payment"] or 0, 2),
        "total_deal_value":  round(ds["total_value"] or 0, 2),
        "cps":               round(total / total_deals, 1) if total_deals else None,
    }


# ---------------------------------------------------------------------------
# /api/rna  — ring no answer summary
# ---------------------------------------------------------------------------

@router.get("/rna")
async def get_rna(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    start_dt, end_dt = _date_range(start, end)
    async with get_db() as db:
        agent_rows = [_row_to_dict(r) for r in await db.fetch(
            """
            SELECT agent, COUNT(*) AS rna_count,
                   AVG(ringtime) AS avg_ringtime,
                   COUNT(DISTINCT queue) AS queues_hit
            FROM ring_no_answer_events
            WHERE event_at BETWEEN $1 AND $2
              AND agent IS NOT NULL AND agent != ''
            GROUP BY agent
            ORDER BY rna_count DESC
            """,
            start_dt, end_dt,
        )]
        total = sum(r["rna_count"] for r in agent_rows)

    # Fallback: use ring_no_answer count from the live /agents/stats cache
    if not total:
        cache_rows = []
        for ag in get_agent_stats_cache():
            name = ag.get("agent") or ""
            rna  = int(ag.get("ring_no_answer") or 0)
            if name and rna > 0:
                cache_rows.append({
                    "agent":       name,
                    "rna_count":   rna,
                    "avg_ringtime": None,
                    "queues_hit":  0,
                    "source":      "cache",
                })
        if cache_rows:
            cache_rows.sort(key=lambda r: r["rna_count"], reverse=True)
            agent_rows = cache_rows
            total = sum(r["rna_count"] for r in cache_rows)

    return {
        "agents": agent_rows,
        "total":  total,
        "as_of":  datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# /api/recordings/{call_id}
# ---------------------------------------------------------------------------

@router.get("/recordings/{call_id}")
async def download_recording(call_id: str):
    from fastapi import HTTPException

    async with get_db() as db:
        row = await db.fetchrow(
            "SELECT raw_json, queue FROM calls WHERE id = $1", call_id
        )

    if not row:
        raise HTTPException(status_code=404, detail="Call not found")

    raw = {}
    try:
        raw = json.loads(row["raw_json"] or "{}")
    except Exception:
        pass

    headers = {"Authorization": f"Bearer {BLUEROCK_API_KEY}"}

    async def _stream(url: str, filename: str):
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url, headers=headers, timeout=30, follow_redirects=True)
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise HTTPException(
                    status_code=404,
                    detail=f"BlueRock returned {exc.response.status_code} for recording"
                )
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"Could not reach BlueRock: {exc}")
            content_type = resp.headers.get("content-type", "audio/mpeg")
            return StreamingResponse(
                resp.aiter_bytes(),
                media_type=content_type,
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

    recording_url = (raw.get("recording") or raw.get("recording_url") or raw.get("recordingfile")
                     or raw.get("record_file") or raw.get("file_url"))
    if recording_url and recording_url.startswith("http"):
        return await _stream(recording_url, f"recording_{call_id[:8]}.mp3")

    uniqueid = (raw.get("unique") or raw.get("uniqueid") or raw.get("callid")
                or raw.get("id") or raw.get("uid") or raw.get("call_id") or raw.get("linkedid"))
    if not uniqueid:
        raise HTTPException(
            status_code=404,
            detail=(
                "Cannot locate recording: no URL field or unique ID found in call record. "
                f"raw_json keys present: {list(raw.keys())}"
            )
        )

    base = BLUEROCK_API_URL.rstrip("/")
    candidates = [
        f"{base}/calls/{uniqueid}/recording",
        f"{base}/recordings/{uniqueid}",
        f"{base}/calls/{uniqueid}/audio",
        f"{base}/call/{uniqueid}/recording",
    ]
    last_error = ""
    async with httpx.AsyncClient() as client:
        for url in candidates:
            try:
                resp = await client.get(url, headers=headers, timeout=30, follow_redirects=True)
                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "audio/mpeg")
                    return StreamingResponse(
                        resp.aiter_bytes(),
                        media_type=content_type,
                        headers={"Content-Disposition": f'attachment; filename="recording_{uniqueid}.mp3"'},
                    )
                last_error = f"{resp.status_code} from {url}"
            except Exception as exc:
                last_error = str(exc)

    raise HTTPException(
        status_code=404,
        detail=(
            f"Recording not found after trying {len(candidates)} URL patterns. "
            f"Last error: {last_error}. Unique ID: {uniqueid}. "
            f"raw_json keys: {list(raw.keys())}"
        )
    )


# ---------------------------------------------------------------------------
# /api/debug/recording/{call_id}
# ---------------------------------------------------------------------------

@router.get("/debug/recording/{call_id}")
async def debug_recording(call_id: str):
    from fastapi import HTTPException

    async with get_db() as db:
        row = await db.fetchrow(
            "SELECT raw_json, queue, agent_name, started_at FROM calls WHERE id = $1",
            call_id,
        )

    if not row:
        raise HTTPException(status_code=404, detail="Call not found")

    raw = {}
    try:
        raw = json.loads(row["raw_json"] or "{}")
    except Exception:
        pass

    return {
        "call_id":    call_id,
        "agent_name": row["agent_name"],
        "queue":      row["queue"],
        "started_at": row["started_at"],
        "raw_json":   raw,
        "raw_keys":   list(raw.keys()),
    }
