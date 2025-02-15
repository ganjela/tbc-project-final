import os
import uuid
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Union
from dotenv import load_dotenv
from producer.user_events import get_random_user_id
from producer.user_events import get_random_item_id
from producer.user_events import extract_item_ids

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MOVIES_FILE_PATH: str = os.getenv("MOVIES_FILE_PATH", "")
ITEM_IDS_PATH: str = os.getenv("ITEM_IDS_PATH", "")
USER_DB_PATH: str = os.getenv("USER_DB_PATH", "")

class AddToCartEvent:
    """
    Represents an 'add to cart' event with user and item details.
    """
    def __init__(self, user_id: int, item_id: str):
        self.user_id = user_id
        self.event_name = "add_to_cart"
        self.timestamp = datetime.now().isoformat()
        self.item_id = item_id
        self.cart_id = str(uuid.uuid4())
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        """
        Returns the event data as a dictionary.
        """
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "item_id": self.item_id,
            "cart_id": self.cart_id,
        }

    @staticmethod
    def to_dict(data: Dict[str, Union[str, int]], ctx: None = None) -> Dict[str, Union[int, str]]:
        """
        Converts the event data to a dictionary format suitable for serialization.
        """
        return {
            "user_id": data.get("user_id"),
            "event_name": data.get("event_name"),
            "timestamp": data.get("timestamp"),
            "item_id": data.get("item_id"),
            "cart_id": data.get("cart_id"),
        }

    def save_to_db(self, db_path: str) -> None:
        """
        Saves the event to the SQLite database.
        
        Args:
            db_path (str): Path to the SQLite database file.
        """
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cart_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        event_name TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        item_id TEXT NOT NULL,
                        cart_id TEXT UNIQUE NOT NULL
                    )
                """)
                cursor.execute("""
                    INSERT INTO cart_events (user_id, event_name, timestamp, item_id, cart_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (self.user_id, self.event_name, self.timestamp, self.item_id, self.cart_id))
                conn.commit()
                logging.info(f"Event saved: {self.cart_id}")
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            raise

def generate_random_add_to_cart_event(db_path: str, items_file: str) -> AddToCartEvent:
    """
    Generates a random AddToCartEvent.

    Args:
        db_path (str): Path to the SQLite database file.
        items_file (str): Path to the file containing item IDs.

    Returns:
        AddToCartEvent: An instance of AddToCartEvent with random user_id and item_id.
    """
    user_id = get_random_user_id(db_path)
    item_id = get_random_item_id(items_file)
    return AddToCartEvent(user_id, item_id)

def main():
    """
    Main function to extract item IDs and generate a sample add-to-cart event.
    """
    if not MOVIES_FILE_PATH or not ITEM_IDS_PATH:
        logging.error("Environment variables MOVIES_FILE_PATH and ITEM_IDS_PATH must be set.")
        return

    extract_item_ids(MOVIES_FILE_PATH, ITEM_IDS_PATH)
    
    try:
        event = generate_random_add_to_cart_event(USER_DB_PATH, ITEM_IDS_PATH)
        event.save_to_db(USER_DB_PATH)
        logging.info(f"Generated event: {event()}")
    except Exception as e:
        logging.error(f"Failed to generate add-to-cart event: {e}")

if __name__ == "__main__":
    main()
