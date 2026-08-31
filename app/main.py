# pyrefly: ignore [missing-import]
from fastapi import FastAPI, HTTPException
# pyrefly: ignore [missing-import]
from pydantic import BaseModel
# pyrefly: ignore [missing-import]
import asyncpg
# pyrefly: ignore [missing-import]
from asyncpg.exceptions import UniqueViolationError
from app.models import CallState

app = FastAPI()
db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    # Connect to the local PostgreSQL Docker container
    db_pool = await asyncpg.create_pool(user='dialer_user', password='dialer_password', database='smart_dialer', host='127.0.0.1')

class WebhookPayload(BaseModel):
    call_id: int
    status: str
    event_id: str

# Define strict legal transitions to handle out-of-order events
VALID_TRANSITIONS = {
    CallState.RINGING.value: [CallState.INITIATED.value],
    CallState.ANSWERED.value: [CallState.RINGING.value],
    CallState.COMPLETED.value: [CallState.CONNECTED.value, CallState.ANSWERED.value]
}

@app.post("/webhook")
async def handle_webhook(payload: WebhookPayload):
    """
    Processes telecom events. Solves duplicates via DB constraints and 
    solves out-of-order events via strict state validation.
    """
    async with db_pool.acquire() as conn:
        try:
            async with conn.transaction():
                # 1. Enforce Idempotency: Insert event ID. If duplicate, UniqueViolationError is caught.
                await conn.execute(
                    "INSERT INTO provider_events_log (provider_event_id, call_id, event_type) VALUES ($1, $2, $3)",
                    payload.event_id, payload.call_id, payload.status
                )
                
                # 2. Enforce State Sequence: Prevent out-of-order events from breaking the system.
                current_state = await conn.fetchval("SELECT state FROM calls WHERE id = $1", payload.call_id)
                
                allowed_previous_states = VALID_TRANSITIONS.get(payload.status, [])
                if current_state not in allowed_previous_states:
                    # Return 200 OK so the provider stops retrying, but silently drop the illegal state change
                    return {"status": "ignored_out_of_order"}

                # 3. Update Call State
                await conn.execute("UPDATE calls SET state = $1, updated_at = NOW() WHERE id = $2", payload.status, payload.call_id)
                
        except UniqueViolationError:
            # The exact same provider event arrived multiple times. Acknowledge but ignore.
            return {"status": "ignored_duplicate"}
            
    return {"status": "processed"}