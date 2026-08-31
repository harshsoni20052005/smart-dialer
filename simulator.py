import argparse
import asyncio
# pyrefly: ignore [missing-import]
import asyncpg
from app.pacing import PacingEngine
from app.safety_controller import SafetyController
from app.models import AgentState, init_db
from app.redis_client import get_redis, close_redis

async def setup_mock_data(pool: asyncpg.Pool):
    """Resets the database and populates 100 agents and 1000 borrowers."""
    await init_db(pool)
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE calls, provider_events_log, agents, borrowers RESTART IDENTITY CASCADE;")
        
        # Insert 100 Agents (assume 30 are on a call, 70 are AVAILABLE)
        await conn.executemany(
            "INSERT INTO agents (name, state) VALUES ($1, $2)",
            [(f"Agent_{i}", AgentState.AVAILABLE.value if i > 30 else AgentState.CONNECTED.value) for i in range(1, 101)]
        )
        
        # Insert 1000 Borrowers
        await conn.executemany(
            "INSERT INTO borrowers (phone_number) VALUES ($1)",
            [(f"+1555000{str(i).zfill(4)}",) for i in range(1, 1001)]
        )
        
    # Also reset and populate Redis state
    redis = await get_redis()
    await redis.flushall()
    # Add agents 31 to 100 to the available pool
    available_agent_ids = [str(i) for i in range(31, 101)]
    if available_agent_ids:
        await redis.sadd("available_agents", *available_agent_ids)

async def run_simulation(scenario: str, answer_rate: float, talk_time: int):
    print(f"\n--- Running Scenario {scenario} ---")
    print(f"Conditions: {answer_rate*100}% Answer Rate | {talk_time}s Avg Talk Time")
    
    pool = await asyncpg.create_pool(user='dialer_user', password='dialer_password', database='smart_dialer', host='127.0.0.1')
    await setup_mock_data(pool)
    
    engine = PacingEngine(answer_rate=answer_rate)
    safety_controller = SafetyController(pool)
    
    # 1. Check real-time database state
    async with pool.acquire() as conn:
        total_agents = await conn.fetchval("SELECT COUNT(*) FROM agents")
        available_agents = await conn.fetchval("SELECT COUNT(*) FROM agents WHERE state = 'AVAILABLE'")
    
    # 2. Pacing Engine requests calls based purely on math
    requested_calls = engine.calculate_desired_calls(available_agents)
    
    # 3. Safety Controller intervenes and sets the real limit
    allowed_calls = await safety_controller.evaluate_pacing_request(requested_calls)
    
    # 4. Output the metrics required by the assignment
    utilization = ((total_agents - available_agents) / total_agents) * 100
    
    print("\n[Metrics Logged]")
    print(f"Agent Utilization:  {utilization:.1f}% (30 connected / 70 available)")
    print(f"Pacing Decision:    Engine requested {requested_calls} calls.")
    if requested_calls > allowed_calls:
        print(f"Safety Controller:  THROTTLED! Reduced from {requested_calls} to {allowed_calls} max allowed calls.")
    else:
        print(f"Safety Controller:  APPROVED. {allowed_calls} calls authorized.")
    print(f"Calls Initiated:    {allowed_calls}")
    print(f"Expected Connected: {int(allowed_calls * answer_rate)}")
    print("-" * 35)

    await pool.close()
    await close_redis()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", type=str, choices=['A', 'B', 'C', 'D'], default='A')
    args = parser.parse_args()

    scenarios = {
        'A': (0.20, 120),
        'B': (0.50, 90),
        'C': (0.70, 180),
        'D': (0.10, 60) # Changing condition: sudden drop in answer rate
    }
    
    rate, time = scenarios[args.scenario]
    asyncio.run(run_simulation(args.scenario, rate, time))