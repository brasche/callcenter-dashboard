"""CRUD endpoints for agent roster management."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.models.database import get_db

router = APIRouter()


class AgentIn(BaseModel):
    full_name: str
    short_name: Optional[str] = None
    bluerock_username: Optional[str] = None
    email: Optional[str] = None
    active: Optional[int] = 1


class AgentUpdate(BaseModel):
    full_name: Optional[str] = None
    short_name: Optional[str] = None
    bluerock_username: Optional[str] = None
    email: Optional[str] = None
    active: Optional[int] = None


async def _all_agents(db):
    rows = await db.fetch(
        "SELECT id, full_name, short_name, bluerock_username, email, active FROM agents ORDER BY full_name"
    )
    return [
        {
            "id":                r["id"],
            "full_name":         r["full_name"],
            "short_name":        r["short_name"],
            "bluerock_username": r["bluerock_username"],
            "email":             r["email"],
            "active":            r["active"],
        }
        for r in rows
    ]


@router.get("")
async def list_agents():
    async with get_db() as db:
        return {"agents": await _all_agents(db)}


@router.post("")
async def create_agent(body: AgentIn):
    async with get_db() as db:
        row = await db.fetchrow(
            """INSERT INTO agents (full_name, short_name, bluerock_username, email, active)
               VALUES ($1, $2, $3, $4, $5)
               RETURNING id""",
            body.full_name, body.short_name, body.bluerock_username, body.email, body.active,
        )
        new_id = row["id"]
        return {"success": True, "id": new_id, "agents": await _all_agents(db)}


@router.put("/{agent_id}")
async def update_agent(agent_id: int, body: AgentUpdate):
    async with get_db() as db:
        fields = {k: v for k, v in body.model_dump().items() if v is not None}
        if not fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        clauses = []
        params = []
        for i, (k, v) in enumerate(fields.items(), start=1):
            clauses.append(f"{k} = ${i}")
            params.append(v)
        params.append(agent_id)
        set_clause = ", ".join(clauses)
        await db.execute(
            f"UPDATE agents SET {set_clause} WHERE id = ${len(fields) + 1}",
            *params,
        )
        return {"success": True, "agents": await _all_agents(db)}


@router.delete("/{agent_id}")
async def delete_agent(agent_id: int):
    async with get_db() as db:
        await db.execute("DELETE FROM agents WHERE id = $1", agent_id)
        return {"success": True, "agents": await _all_agents(db)}
