# SmartDialer Prototype

A functional SmartDialer prototype integrating a Predictive Pacing Engine regulated by a strict Safety Controller.

## Setup Instructions

**1. Start the Database & Redis Cache**
The system relies on PostgreSQL for ACID-compliant borrower state management, and Redis for high-speed, lock-free agent allocation.
`docker-compose up -d`

**2. Install Dependencies**
`python -m venv venv`
`source venv/bin/activate` # On Windows: `venv\Scripts\activate`
`pip install -r requirements.txt`

**3. Run the API (Webhooks & Mock Providers)**
`uvicorn app.main:app --reload`

**4. Run the Simulator**
Execute the required scenarios (A, B, C, D) to observe pacing and safety decisions.
`python simulator.py --scenario A`

**5. Run the Load Test**
Verify the Redis Lua atomic allocator handles 500 concurrent worker collisions effortlessly:
`python load_test.py`