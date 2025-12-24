"""
Event-Driven Audit Log Service
Single-file FastAPI application

Features:
- Centralized audit logging
- Append-only event storage
- Actor / action / resource modeling
- Query & filtering
- SQLite persistence
- Production-grade backend pattern

Run:
pip install fastapi uvicorn
uvicorn app:app --reload
"""

import sqlite3
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Audit Log Service")
DB_PATH = "audit_logs.db"

# -------------------------
# Database
# -------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            resource TEXT NOT NULL,
            metadata TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

init_db()

def get_db():
    return sqlite3.connect(DB_PATH)

# -------------------------
# Models
# -------------------------
class AuditEvent(BaseModel):
    actor: str
    action: str
    resource: str
    metadata: Optional[str] = None

class AuditLogOut(BaseModel):
    actor: str
    action: str
    resource: str
    metadata: Optional[str]
    created_at: str

# -------------------------
# Event Publisher
# -------------------------
def publish_event(event: AuditEvent):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO audit_logs (actor, action, resource, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
        (
            event.actor,
            event.action,
            event.resource,
            event.metadata,
            datetime.utcnow().isoformat()
        )
    )
    conn.commit()
    conn.close()

# -------------------------
# API Routes
# -------------------------
@app.post("/events")
def create_event(event: AuditEvent):
    publish_event(event)
    return {"status": "event_logged"}

@app.get("/logs", response_model=List[AuditLogOut])
def query_logs(
    actor: Optional[str] = None,
    action: Optional[str] = None,
    resource: Optional[str] = None,
    limit: int = 50
):
    conn = get_db()
    cur = conn.cursor()

    query = "SELECT actor, action, resource, metadata, created_at FROM audit_logs WHERE 1=1"
    params = []

    if actor:
        query += " AND actor = ?"
        params.append(actor)
    if action:
        query += " AND action = ?"
        params.append(action)
    if resource:
        query += " AND resource = ?"
        params.append(resource)

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    rows = cur.execute(query, params).fetchall()
    conn.close()

    return [
        {
            "actor": r[0],
            "action": r[1],
            "resource": r[2],
            "metadata": r[3],
            "created_at": r[4]
        }
        for r in rows
    ]

@app.get("/health")
def health():
    return {"status": "ok"}
