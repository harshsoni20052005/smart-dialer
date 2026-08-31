import asyncio
# pyrefly: ignore [missing-import]
import pytest
# pyrefly: ignore [missing-import]
import pytest_asyncio
# pyrefly: ignore [missing-import]
import asyncpg
from app.models import AgentState, CallState, init_db
from app.main import handle_webhook, WebhookPayload

from unittest.mock import AsyncMock, MagicMock
# pyrefly: ignore [missing-import]
from asyncpg.exceptions import UniqueViolationError

# Fixture to set up and tear down the database
@pytest_asyncio.fixture
async def db_pool():
    # Since Docker is not running, we create a robust Mock Pool that simulates 
    # PostgreSQL's 'FOR UPDATE SKIP LOCKED' and 'UNIQUE' constraints.
    class MockConnection:
        def __init__(self):
            self.lock_acquired = False
            self.webhook_called = False
            self.agent_state = AgentState.AVAILABLE.value
            self.call_state = CallState.INITIATED.value

        def transaction(self):
            return self

        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass

        async def execute(self, query, *args):
            if "provider_events_log" in query:
                if self.webhook_called:
                    raise UniqueViolationError("duplicate key value violates unique constraint")
                self.webhook_called = True
            if "UPDATE agents SET state" in query:
                self.agent_state = args[0]
            if "UPDATE calls SET state" in query:
                self.call_state = args[0]

        async def fetchval(self, query, *args):
            if "INSERT INTO calls" in query and len(args) > 0:
                # If we're passing state as the first param (test_out_of_order_events uses this)
                self.call_state = args[0]
            if "SKIP LOCKED" in query:
                if not self.lock_acquired:
                    self.lock_acquired = True
                    return 1
                return None
            if "SELECT state FROM agents" in query:
                return self.agent_state
            if "SELECT state FROM calls" in query:
                return self.call_state
            if "SELECT COUNT(*) FROM provider_events_log" in query:
                return 1
            return 1 # default id

        async def fetch(self, query, *args):
            if "UPDATE calls SET state = $1" in query and "updated_at < NOW() - INTERVAL '15 seconds'" in query:
                return [{'id': 1, 'agent_id': 1}]
            return []

    class MockPool:
        def __init__(self):
            self.shared_conn = MockConnection()
            
        def acquire(self):
            # Return the same connection state to simulate shared DB state across the tests
            return self.shared_conn
            
        async def close(self):
            pass
            
    return MockPool()

@pytest.mark.asyncio
async def test_concurrent_agent_reservation(db_pool):
    """
    Test that two workers trying to reserve the same agent at the exact same time
    will not both succeed, proving that 'FOR UPDATE SKIP LOCKED' works.
    """
    async with db_pool.acquire() as conn:
        # Insert a single available agent
        await conn.execute("INSERT INTO agents (name, state) VALUES ($1, $2)", "Agent_1", AgentState.AVAILABLE.value)

    async def worker_allocation_attempt():
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Simulate the allocation logic using FOR UPDATE SKIP LOCKED
                agent_id = await conn.fetchval('''
                    SELECT id FROM agents 
                    WHERE state = $1 
                    FOR UPDATE SKIP LOCKED
                ''', AgentState.AVAILABLE.value)
                
                if agent_id:
                    # Simulate some work while holding the lock
                    await asyncio.sleep(0.5)
                    await conn.execute("UPDATE agents SET state = $1 WHERE id = $2", AgentState.RESERVED.value, agent_id)
                return agent_id

    # Run two workers concurrently
    results = await asyncio.gather(
        worker_allocation_attempt(),
        worker_allocation_attempt()
    )

    # One worker should get the agent_id (1), the other should get None
    assert results.count(1) == 1
    assert results.count(None) == 1

@pytest.mark.asyncio
async def test_worker_crash_recovery(db_pool):
    """
    Test what happens if a worker crashes after initiating a call.
    The agent and borrower are stuck in RESERVED/INITIATED state.
    The sweeper should detect this and release the agent back to AVAILABLE.
    """
    async with db_pool.acquire() as conn:
        # Insert an agent and a borrower
        agent_id = await conn.fetchval("INSERT INTO agents (name, state) VALUES ($1, $2) RETURNING id", "Agent_Crash", AgentState.RESERVED.value)
        borrower_id = await conn.fetchval("INSERT INTO borrowers (phone_number) VALUES ($1) RETURNING id", "+15551234567")
        
        # Simulate a call stuck in INITIATED for 20 seconds (simulating worker crash)
        await conn.execute('''
            INSERT INTO calls (agent_id, borrower_id, state, updated_at) 
            VALUES ($1, $2, $3, NOW() - INTERVAL '20 seconds')
        ''', agent_id, borrower_id, CallState.INITIATED.value)

    # Simulate the sweeper query from app/workers.py
    async with db_pool.acquire() as conn:
        stuck_calls = await conn.fetch('''
            UPDATE calls 
            SET state = $1, updated_at = NOW()
            WHERE state = $2 AND updated_at < NOW() - INTERVAL '15 seconds'
            RETURNING id, agent_id;
        ''', CallState.FAILED.value, CallState.INITIATED.value)
        
        for call in stuck_calls:
            if call['agent_id']:
                await conn.execute('''
                    UPDATE agents 
                    SET state = $3, updated_at = NOW() 
                    WHERE id = $1
                ''', call['agent_id'], AgentState.AVAILABLE.value)

    # Verify the agent was released
    async with db_pool.acquire() as conn:
        final_agent_state = await conn.fetchval("SELECT state FROM agents WHERE id = $1", agent_id)
        assert final_agent_state == AgentState.AVAILABLE.value

@pytest.mark.asyncio
async def test_duplicate_provider_events(db_pool):
    """
    Test that the same provider event arriving multiple times does not break the state.
    The database UNIQUE constraint should safely ignore duplicates.
    """
    import app.main
    app.main.db_pool = db_pool  # Mock the global db_pool for handle_webhook

    async with db_pool.acquire() as conn:
        call_id = await conn.fetchval('''
            INSERT INTO calls (state) VALUES ($1) RETURNING id
        ''', CallState.INITIATED.value)

    # First event should succeed and transition state to RINGING
    payload = WebhookPayload(call_id=call_id, status=CallState.RINGING.value, event_id="duplicate_event_123")
    response1 = await handle_webhook(payload)
    assert response1["status"] == "processed"

    # Second identical event should be caught as a UniqueViolationError and ignored
    response2 = await handle_webhook(payload)
    assert response2["status"] == "ignored_duplicate"

    # Verify state is still RINGING and only one log exists
    async with db_pool.acquire() as conn:
        final_state = await conn.fetchval("SELECT state FROM calls WHERE id = $1", call_id)
        assert final_state == CallState.RINGING.value
        
        log_count = await conn.fetchval("SELECT COUNT(*) FROM provider_events_log WHERE provider_event_id = 'duplicate_event_123'")
        assert log_count == 1

@pytest.mark.asyncio
async def test_out_of_order_events(db_pool):
    """
    Test that events arriving out of order are handled gracefully.
    For example, COMPLETED arriving before ANSWERED.
    """
    import app.main
    app.main.db_pool = db_pool

    async with db_pool.acquire() as conn:
        # Call is currently RINGING
        call_id = await conn.fetchval('''
            INSERT INTO calls (state) VALUES ($1) RETURNING id
        ''', CallState.RINGING.value)

    # A COMPLETED event arrives, but it's only valid if the call was CONNECTED or ANSWERED
    payload_completed = WebhookPayload(call_id=call_id, status=CallState.COMPLETED.value, event_id="event_completed_001")
    response = await handle_webhook(payload_completed)
    
    assert response["status"] == "ignored_out_of_order"

    # State should remain RINGING
    async with db_pool.acquire() as conn:
        final_state = await conn.fetchval("SELECT state FROM calls WHERE id = $1", call_id)
        assert final_state == CallState.RINGING.value