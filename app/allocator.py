import asyncpg
from app.models import AgentState, CallState
from app.redis_client import get_redis

# Atomic Lua script: Pops an agent from available set and moves to reserved set
ALLOCATE_AGENT_LUA = """
local agent_id = redis.call('SPOP', KEYS[1])
if agent_id then
    redis.call('SADD', KEYS[2], agent_id)
    return agent_id
end
return nil
"""

class CallAllocator:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def allocate_and_initiate(self, allowed_calls: int):
        """
        Atomically locks agents using Redis Lua scripts for extreme scale,
        while reserving borrowers in PostgreSQL via SKIP LOCKED.
        """
        initiated_calls = []
        redis = await get_redis()
        
        allocate_agent_script = redis.register_script(ALLOCATE_AGENT_LUA)
        
        async with self.pool.acquire() as conn:
            for _ in range(allowed_calls):
                # 1. Lock an available agent using Redis (Atomic & Lock-free, No DB contention)
                agent_id_bytes = await allocate_agent_script(keys=['available_agents', 'reserved_agents'])
                
                if not agent_id_bytes:
                    break # No more agents available
                    
                agent_id = int(agent_id_bytes)

                # 2. Lock an uncontacted borrower
                async with conn.transaction():
                    borrower_id = await conn.fetchval('''
                        SELECT id FROM borrowers 
                        WHERE is_contacted = FALSE 
                        FOR UPDATE SKIP LOCKED
                    ''')

                    if not borrower_id:
                        # Rollback agent in Redis if we run out of borrowers
                        await redis.srem('reserved_agents', agent_id)
                        await redis.sadd('available_agents', agent_id)
                        break

                    # 3. Reserve them and create the call in Postgres
                    await conn.execute("UPDATE borrowers SET is_contacted = TRUE WHERE id = $1", borrower_id)
                    
                    call_id = await conn.fetchval('''
                        INSERT INTO calls (agent_id, borrower_id, state) 
                        VALUES ($1, $2, $3) RETURNING id
                    ''', agent_id, borrower_id, CallState.INITIATED.value)
                    
                    initiated_calls.append((call_id, borrower_id))
                    
        return initiated_calls