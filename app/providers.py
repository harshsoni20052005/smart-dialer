import asyncio
import random
# pyrefly: ignore [missing-import]
import httpx

class BaseProvider:
    async def place_call(self, call_id: int, borrower_id: int):
        raise NotImplementedError

class ProviderA(BaseProvider):
    """Fast, reliable, and predictable provider."""
    async def place_call(self, call_id: int, borrower_id: int):
        asyncio.create_task(self._simulate_webhook(call_id))
    
    async def _simulate_webhook(self, call_id: int):
        await asyncio.sleep(0.5)  # Fast setup
        async with httpx.AsyncClient() as client:
            await client.post("http://127.0.0.1:8000/webhook", json={"call_id": call_id, "status": "RINGING", "event_id": f"A_{call_id}_RING"})
            await asyncio.sleep(1)
            await client.post("http://127.0.0.1:8000/webhook", json={"call_id": call_id, "status": "ANSWERED", "event_id": f"A_{call_id}_ANS"})

class ProviderB(BaseProvider):
    """Slower provider with duplicates and out-of-order events."""
    async def place_call(self, call_id: int, borrower_id: int):
        asyncio.create_task(self._simulate_webhook(call_id))

    async def _simulate_webhook(self, call_id: int):
        await asyncio.sleep(random.uniform(2, 5))  # Occasional timeouts/slow latency
        async with httpx.AsyncClient() as client:
            # Send duplicate events to test idempotency
            await client.post("http://127.0.0.1:8000/webhook", json={"call_id": call_id, "status": "RINGING", "event_id": f"B_{call_id}_RING"})
            await client.post("http://127.0.0.1:8000/webhook", json={"call_id": call_id, "status": "RINGING", "event_id": f"B_{call_id}_RING"})
            
            # 50% chance to send events arriving out of order (COMPLETED before ANSWERED)
            if random.random() > 0.5:
                await client.post("http://127.0.0.1:8000/webhook", json={"call_id": call_id, "status": "COMPLETED", "event_id": f"B_{call_id}_COMP"})
                await client.post("http://127.0.0.1:8000/webhook", json={"call_id": call_id, "status": "ANSWERED", "event_id": f"B_{call_id}_ANS"})
            else:
                await client.post("http://127.0.0.1:8000/webhook", json={"call_id": call_id, "status": "ANSWERED", "event_id": f"B_{call_id}_ANS"})