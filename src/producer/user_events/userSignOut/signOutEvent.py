from datetime import datetime
from typing import Dict, Union
import random
import logging

class SignOutEvent:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.event_name = "sign_out"
        self.timestamp = datetime.now().isoformat()
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def to_dict(data) -> Dict[str, Union[int, str]]:
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
        }

def generate_random_sign_out():
    user_id = random.randint(1, 100000)
    return SignOutEvent(user_id)