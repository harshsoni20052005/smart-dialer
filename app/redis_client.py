import redis.asyncio as redis

# Global Redis pool
redis_pool = None

async def init_redis():
    global redis_pool
    # Connect to the local Redis Docker container
    redis_pool = redis.from_url("redis://127.0.0.1:6379", decode_responses=True)
    return redis_pool

async def get_redis():
    global redis_pool
    if not redis_pool:
        await init_redis()
    return redis_pool

async def close_redis():
    global redis_pool
    if redis_pool:
        await redis_pool.aclose()
