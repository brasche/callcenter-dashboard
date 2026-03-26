"""
Database viewer endpoints — read-only inspection of all tables.
"""

from fastapi import APIRouter, Query
from typing import Optional
from app.models.database import get_db

router = APIRouter()

ALLOWED_TABLES = {
    "calls", "deals", "agents", "live_calls", "sync_log",
    "agent_status_snapshots", "agent_state_blocks", "agent_daily_stats",
}


@router.get("/tables")
async def list_tables():
    async with get_db() as db:
        rows = await db.fetch(
            "SELECT tablename AS name FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        tables = [r["name"] for r in rows]

        counts = {}
        for t in tables:
            if t in ALLOWED_TABLES:
                counts[t] = await db.fetchval(f"SELECT COUNT(*) FROM {t}")

    return {"tables": tables, "counts": counts}


@router.get("/table/{table_name}")
async def get_table(
    table_name: str,
    limit:  int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None),
):
    if table_name not in ALLOWED_TABLES:
        return {"error": f"Table '{table_name}' is not accessible"}

    async with get_db() as db:
        # Column names from information_schema
        col_rows = await db.fetch(
            """SELECT column_name FROM information_schema.columns
               WHERE table_name = $1 AND table_schema = 'public'
               ORDER BY ordinal_position""",
            table_name,
        )
        cols = [r["column_name"] for r in col_rows]

        # Build query with optional search
        where = ""
        params: list = []
        if search:
            clauses = [f"CAST({c} AS TEXT) LIKE ${i+1}" for i, c in enumerate(cols)]
            where = "WHERE " + " OR ".join(clauses)
            params = [f"%{search}%"] * len(cols)

        total = await db.fetchval(
            f"SELECT COUNT(*) FROM {table_name} {where}", *params
        )

        limit_param  = f"${len(params) + 1}"
        offset_param = f"${len(params) + 2}"
        data_sql = f"SELECT * FROM {table_name} {where} LIMIT {limit_param} OFFSET {offset_param}"
        rows = await db.fetch(data_sql, *params, limit, offset)

    return {
        "table":   table_name,
        "columns": cols,
        "rows":    [list(r) for r in rows],
        "total":   total,
        "limit":   limit,
        "offset":  offset,
    }
