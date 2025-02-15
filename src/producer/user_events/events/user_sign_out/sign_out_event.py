import logging
import random
from datetime import datetime
from typing import Any, Dict, Union

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SignOutEvent:
    """
    Represents a user sign-out event.

    Attributes:
        user_id (int): The unique identifier of the user.
        event_name (str): The name of the event, defaults to 'sign_out'.
        timestamp (str): The ISO-formatted timestamp when the event was created.
    """
    def __init__(self, user_id: int) -> None:
        self.user_id = user_id
        self.event_name = "sign_out"
        self.timestamp = datetime.now().isoformat()
    
    def __call__(self) -> Dict[str, Union[int, str]]:
        """
        Returns the event data as a dictionary.

        Returns:
            dict: A dictionary containing user_id, event_name, and timestamp.
        """
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def to_dict(data: Dict[str, Union[int, str]], ctx: Any = None) -> Dict[str, Union[int, str]]:
        """
        Converts the provided data into a dictionary suitable for serialization.

        Args:
            data (dict): The event data to convert.
            ctx (Any, optional): Additional context, defaults to None.

        Returns:
            dict: The converted dictionary with keys 'user_id', 'event_name', and 'timestamp'.
        """
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
        }

def generate_random_sign_out() -> SignOutEvent:
    """
    Generates a SignOutEvent with a random user_id.

    Returns:
        SignOutEvent: An instance of SignOutEvent with a random user_id.
    """
    user_id = random.randint(1, 100_000)
    event = SignOutEvent(user_id)
    logging.info("Generated sign-out event: %s", event())
    return event
