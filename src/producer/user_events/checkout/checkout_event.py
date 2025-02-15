import logging
import random
from datetime import datetime
from typing import Dict, Union
from producer.user_events import get_random_user_id, get_random_cart_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CheckoutEvent:
    """
    Represents a checkout event for a cart.
    """
    def __init__(self, user_id: int, cart_id: str, payment_method: str) -> None:
        self.user_id = user_id
        self.event_name = "checkout_to_cart"
        self.timestamp = datetime.now().isoformat()
        self.cart_id = cart_id
        self.payment_method = payment_method
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        """
        Returns the event data as a dictionary.
        """
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "cart_id": self.cart_id,
            "payment_method": self.payment_method,
        }
    
    @staticmethod
    def to_dict(data: Dict[str, Union[str, int]], ctx=None) -> Dict[str, Union[int, str]]:
        """
        Serializes the event data into a dictionary format.
        
        Args:
            data (Dict[str, Union[str, int]]): The event data.
            ctx: Optional context for serialization.
        
        Returns:
            Dict[str, Union[int, str]]: Serialized event data.
        """
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
            "cart_id": data.get("cart_id"),
            "payment_method": data.get("payment_method"),
        }

def generate_random_checkout(db_path: str) -> CheckoutEvent:
    """
    Generates a random checkout event using random user and cart IDs,
    along with a randomly selected payment method.
    
    Args:
        db_path (str): The database path used to obtain random user and cart IDs.
    
    Returns:
        CheckoutEvent: An instance of CheckoutEvent with randomized attributes.
    """
    user_id = get_random_user_id(db_path)
    cart_id = get_random_cart_id(db_path)
    payment_method = random.choice(["Cash", "Card"])
    return CheckoutEvent(user_id, cart_id, payment_method)
