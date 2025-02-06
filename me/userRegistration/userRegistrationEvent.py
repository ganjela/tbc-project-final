from datetime import datetime
from typing import Dict, Union
import random

class UserRegistration:
    def __init__(self, user_id: int, age: int, masked_email: str, preferred_language: str):
        self.user_id = user_id
        self.event_name = "consumer_registration"
        self.timestamp = datetime.now().isoformat()
        self.age = age
        self.masked_email = self.mask_email(masked_email)
        self.preferred_language = preferred_language
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "age": self.age,
            "masked_email": self.masked_email,
            "preferred_language": self.preferred_language,
        }

    @staticmethod
    def to_dict(data) -> Dict[str, Union[int, str]]:
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
            "age": data.get("age"),
            "masked_email": data.get("masked_email"),
            "preferred_language": data.get("preferred_language"),
        }
    
    @staticmethod
    def mask_email(masked_email: str) -> str:
        username, domain = masked_email.split("@")
        return f"{username[0]}***@{domain}"

def generate_random_registration():
    user_id = random.randint(1, 100000)
    age = random.randint(18, 70)
    masked_email = f"user{user_id}@example.com"
    preferred_language = random.choice(["English", "Spanish", "French", "German", "Chinese"])
    
    return UserRegistration(user_id, age, masked_email, preferred_language)
