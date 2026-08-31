import enum
import asyncpg
from datetime import datetime

# --- Strict State Machines ---

class AgentState(str, enum.Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP UP"
    PAUSED = "PAUSED"

class CallState(str, enum.Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

# --- Database Schema Initialization ---

async def init_db(pool: asyncpg.Pool):
    """
    Creates the required relational tables. 
    The UNIQUE constraint on provider_events_log natively solves duplicate provider events.
    """
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS agents (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                state VARCHAR(20) DEFAULT 'OFFLINE',
                last_heartbeat TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS borrowers (
                id SERIAL PRIMARY KEY,
                phone_number VARCHAR(20) UNIQUE,
                is_contacted BOOLEAN DEFAULT FALSE
            );

            CREATE TABLE IF NOT EXISTS calls (
                id SERIAL PRIMARY KEY,
                agent_id INTEGER REFERENCES agents(id),
                borrower_id INTEGER REFERENCES borrowers(id),
                state VARCHAR(20) DEFAULT 'QUEUED',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS provider_events_log (
                id SERIAL PRIMARY KEY,
                provider_event_id VARCHAR(100) UNIQUE, -- Crucial for idempotency
                call_id INTEGER REFERENCES calls(id),
                event_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );
        ''')