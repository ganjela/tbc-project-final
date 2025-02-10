from datetime import datetime
from typing import Dict, Union
import random
import logging

class CheckoutEvent:
    def __init__(self, user_id: int, cart_id: str, payment_method: str):
        self.user_id = user_id
        self.event_name = "checkout_to_cart"
        self.timestamp = datetime.now().isoformat()
        self.cart_id = cart_id
        self.payment_method = payment_method
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "cart_id": self.cart_id,
            "payment_method": self.payment_method,
        }

    @staticmethod
    def to_dict(data) -> Dict[str, Union[int, str]]:
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
            "cart_id": data.get("cart_id"),
            "payment_method": data.get("payment_method"),
        }

def generate_random_checkout(users: list, cart_ids: list):
    user_id = random.choice(users)
    cart_id = random.choice(cart_ids)
    payment_method = random.choice(["Cash", "Card"])
    return CheckoutEvent(user_id, cart_id, payment_method)
