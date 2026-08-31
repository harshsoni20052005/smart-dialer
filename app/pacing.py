class PacingEngine:
    def __init__(self, answer_rate: float = 0.20):
        self.answer_rate = answer_rate

    def calculate_desired_calls(self, apparent_available_agents: int) -> int:
        """
        Calculates how many calls to initiate based on the estimated answer rate.
        If the answer rate is 20%, it takes 5 calls to get 1 connection.
        """
        if apparent_available_agents <= 0:
            return 0
        
        # Predictive logic: Request enough calls to get connected calls equal to available agents.
        desired_calls = int(apparent_available_agents / self.answer_rate)
        return desired_calls