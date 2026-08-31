# SmartDialer Prototype

A functional SmartDialer prototype integrating a Predictive Pacing Engine regulated by a strict Safety Controller.

## Setup Instructions

**1. Start the Database**
The system relies on PostgreSQL for ACID-compliant state management and concurrency locking.
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