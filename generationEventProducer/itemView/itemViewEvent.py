from datetime import datetime
from typing import Dict, Union
import random
import logging

class ItemViewEvent:
    def __init__(self, user_id: int, item_name: str):
        self.user_id = user_id
        self.event_name = "item_view"
        self.timestamp = datetime.now().isoformat()
        self.item_name = item_name
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "item_name": self.item_name,
        }

    @staticmethod
    def to_dict(data) -> Dict[str, Union[int, str]]:
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
            "item_name": data.get("item_name"),
        }

def load_items_from_file(filename: str) -> list:
    with open(filename, "r") as file:
        return [line.strip() for line in file.readlines()]

def generate_random_item_view(users: list, items: list):
    user_id = random.choice(users)
    item_name = random.choice(items)
    return ItemViewEvent(user_id, item_name)