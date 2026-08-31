import asyncio
# pyrefly: ignore [missing-import]
import asyncpg
from app.models import AgentState, CallState

async def stale_state_sweeper(pool: asyncpg.Pool):
    """
    Background task that runs continuously to clean up after worker crashes 
    and detect sudden agent network drops.
    """
    while True:
        try:
            async with pool.acquire() as conn:
                # 1. Recover from Worker Crashes
                # If a worker reserves an agent/borrower, initiates a call, and immediately crashes, 
                # the call stays stuck in INITIATED. This query detects calls unchanged for 15 seconds.
                stuck_calls = await conn.fetch('''
                    UPDATE calls 
                    SET state = $1, updated_at = NOW()
                    WHERE state = $2 AND updated_at < NOW() - INTERVAL '15 seconds'
                    RETURNING id, agent_id;
                ''', CallState.FAILED.value, CallState.INITIATED.value)
                
                # Release the locked agents back into the pool so they aren't stuck forever
                for call in stuck_calls:
                    if call['agent_id']:
                        await conn.execute('''
                            UPDATE agents 
                            SET state = $3, updated_at = NOW() 
                            WHERE id = $1
                        ''', call['agent_id'], AgentState.AVAILABLE.value)

                # 2. Handle Sudden Agent Drops
                # If 40 out of 100 agents suddenly lose connection, their frontend stops sending heartbeats.
                # This query sweeps for missing heartbeats and instantly marks them OFFLINE.
                await conn.execute('''
                    UPDATE agents 
                    SET state = $1, updated_at = NOW() 
                    WHERE state = $2 AND last_heartbeat < NOW() - INTERVAL '10 seconds'
                ''', AgentState.OFFLINE.value, AgentState.AVAILABLE.value)
                
        except Exception as e:
            print(f"Sweeper encountered an error: {e}")
        
        await asyncio.sleep(5) # Run this check every 5 seconds