import logging
import random
from datetime import datetime
from typing import Dict, Union, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SignInEvent:
    """
    Represents a sign-in event for a user.
    """
    def __init__(self, user_id: int) -> None:
        self.user_id = user_id
        self.event_name = "sign_in"
        self.timestamp = datetime.now().isoformat()
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        """
        Returns the sign-in event data as a dictionary.
        """
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def to_dict(data: Dict[str, Union[str, int]], ctx: Any = None) -> Dict[str, Union[str, int]]:
        """
        Serializes the sign-in event data for use with serializers.

        Args:
            data (Dict[str, Union[str, int]]): Event data to be serialized.
            ctx (Any, optional): Serialization context (unused).

        Returns:
            Dict[str, Union[str, int]]: Serialized event data.
        """
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
        }

def generate_random_sign_in() -> SignInEvent:
    """
    Generates a random sign-in event with a random user ID.

    Returns:
        SignInEvent: An instance of SignInEvent with a randomly generated user ID.
    """
    user_id = random.randint(1, 100000)
    return SignInEvent(user_id)
