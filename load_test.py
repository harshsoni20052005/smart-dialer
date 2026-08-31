import asyncio
import time
import asyncpg
from app.models import AgentState, CallState, init_db
from app.allocator import CallAllocator
from app.redis_client import get_redis, close_redis

async def setup_load_test_db(pool: asyncpg.Pool):
    """Initializes 100 available agents and 1000 uncontacted borrowers."""
    await init_db(pool)
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE calls, provider_events_log, agents, borrowers RESTART IDENTITY CASCADE;")
        
        # Insert exactly 100 Available Agents
        await conn.executemany(
            "INSERT INTO agents (name, state) VALUES ($1, $2)",
            [(f"Agent_Load_{i}", AgentState.AVAILABLE.value) for i in range(1, 101)]
        )
        
        # Insert 1000 Borrowers
        await conn.executemany(
            "INSERT INTO borrowers (phone_number) VALUES ($1)",
            [(f"+1555900{str(i).zfill(4)}",) for i in range(1, 1001)]
        )
        
    redis = await get_redis()
    await redis.flushall()
    # Add agents 1 to 100 to the available pool
    available_agent_ids = [str(i) for i in range(1, 101)]
    if available_agent_ids:
        await redis.sadd("available_agents", *available_agent_ids)

async def worker_task(worker_id: int, allocator: CallAllocator):
    """Simulates a worker attempting to allocate 1 agent concurrently."""
    try:
        # Each worker asks for 1 call
        result = await allocator.allocate_and_initiate(allowed_calls=1)
        return result
    except Exception as e:
        print(f"Worker {worker_id} crashed: {e}")
        return []

async def run_load_test():
    print("--- Starting Load Test ---")
    print("Simulating 500 concurrent workers fighting over 100 available agents...")
    
    try:
        # We need a slightly higher max_size for the pool to support 500 concurrent asyncio tasks
        # without connection starvation, but postgres max_connections is usually 100 default.
        # We'll use 50 connections to bottleneck the app and force lock contention.
        pool = await asyncpg.create_pool(
            user='dialer_user', password='dialer_password', database='smart_dialer', host='127.0.0.1',
            min_size=10, max_size=50
        )
    except ConnectionRefusedError:
        print("\n[!] FATAL: ConnectionRefusedError.")
        print("Please start the PostgreSQL database (docker-compose up -d) before running the load test.")
        return
        
    await setup_load_test_db(pool)
    allocator = CallAllocator(pool)
    
    start_time = time.time()
    
    # Spawn 500 concurrent workers
    tasks = [worker_task(i, allocator) for i in range(500)]
    results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    
    # Flatten the results list
    successful_allocations = [item for sublist in results for item in sublist]
    
    redis = await get_redis()
    reserved_agents_count = await redis.scard("reserved_agents")
    available_agents_count = await redis.scard("available_agents")
    
    async with pool.acquire() as conn:
        initiated_calls_count = await conn.fetchval("SELECT COUNT(*) FROM calls WHERE state = $1", CallState.INITIATED.value)
        
    print(f"\n[Load Test Results]")
    print(f"Total time for 500 workers: {end_time - start_time:.2f} seconds")
    print(f"Total calls successfully initiated: {len(successful_allocations)}")
    
    print("\n[Database Assertions]")
    print(f"Reserved Agents in DB: {reserved_agents_count} (Expected: 100)")
    print(f"Initiated Calls in DB: {initiated_calls_count} (Expected: 100)")
    print(f"Available Agents remaining: {available_agents_count} (Expected: 0)")
    
    assert len(successful_allocations) == 100, "Race condition! Allocated more/less calls than available agents."
    assert reserved_agents_count == 100, "Race condition! Agent states not cleanly reserved."
    
    print("\nSUCCESS: The Lua Redis allocator successfully prevented all race conditions under heavy load!")
    
    await pool.close()
    await close_redis()

if __name__ == "__main__":
    asyncio.run(run_load_test())
