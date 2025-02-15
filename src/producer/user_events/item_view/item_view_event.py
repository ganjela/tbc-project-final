from datetime import datetime
from typing import Dict, Union, Any
import logging
from producer.user_events import get_random_user_id, get_random_item_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ItemViewEvent:
    """
    Represents an event where a user views an item.
    """
    def __init__(self, user_id: int, item_id: str) -> None:
        self.user_id = user_id
        self.event_name = "item_view"
        self.timestamp = datetime.now().isoformat()
        self.item_id = item_id
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        """
        Returns the event data as a dictionary.
        """
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "item_id": self.item_id,
        }

    @staticmethod
    def to_dict(data: Dict[str, Union[str, int]], ctx: Any = None) -> Dict[str, Union[str, int]]:
        """
        Serializes event data for use in message serialization.

        Args:
            data (Dict[str, Union[str, int]]): Event data dictionary.
            ctx (Any, optional): Optional serialization context.

        Returns:
            Dict[str, Union[str, int]]: Serialized event data.
        """
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
            "item_id": data.get("item_id"),
        }

def generate_random_item_view(db_path: str, items_file: str) -> ItemViewEvent:
    """
    Generates a random item view event by selecting a random user and a random item.

    Args:
        db_path (str): Path to the database used to obtain a random user.
        items_file (str): Path to the file used to obtain a random item.

    Returns:
        ItemViewEvent: A newly created item view event.
    """
    user_id = get_random_user_id(db_path)
    item_id = get_random_item_id(items_file)
    return ItemViewEvent(user_id, item_id)
