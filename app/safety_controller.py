import asyncpg
from app.redis_client import get_redis

class SafetyController:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def evaluate_pacing_request(self, requested_calls: int) -> int:
        """
        Intervenes on the Pacing Engine's request to enforce progressive safety guarantees.
        Now uses Redis for O(1) lock-free available agent counting.
        """
        redis = await get_redis()
        
        # Query actual available agents from Redis instead of PostgreSQL lock pool
        available_agents = await redis.scard('available_agents')
            
        # The ultimate progressive safety net: never initiate more calls than available agents
        if requested_calls > available_agents:
            return available_agents # Reduce the number (fallback to progressive)
            
        return requested_calls # Approve
