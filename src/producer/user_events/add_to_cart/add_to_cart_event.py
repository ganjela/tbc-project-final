import os
import random
import uuid
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Union
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MOVIES_FILE_PATH: str = os.getenv("MOVIES_FILE_PATH", "")
ITEM_IDS_PATH: str = os.getenv("ITEM_IDS_PATH", "")

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

def extract_item_ids(input_file: str, output_file: str) -> None:
    """
    Extracts numeric ITEM IDs from the input file and writes them to the output file.

    Args:
        input_file (str): Path to the input file containing item data.
        output_file (str): Path to the output file to write extracted item IDs.
    """
    try:
        with open(input_file, 'r', errors='ignore') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                if line.startswith("ITEM"):
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        outfile.write(parts[1] + "\n")
        logging.info(f"Successfully extracted item IDs from {input_file} to {output_file}.")
    except FileNotFoundError:
        logging.error(f"File not found: {input_file}")
    except IOError as e:
        logging.error(f"I/O error({e.errno}): {e.strerror}")

def select_random_item_id(filename: str) -> str:
    """
    Selects a random ITEM ID from the specified file using reservoir sampling.

    Args:
        filename (str): Path to the file containing item IDs.

    Returns:
        str: A randomly selected item ID.

    Raises:
        ValueError: If no ITEM IDs are found in the file.
    """
    selected_item = None
    try:
        with open(filename, 'r') as file:
            for i, line in enumerate(file, start=1):
                if random.randrange(i) == 0:
                    selected_item = line.strip()
        if selected_item is None:
            raise ValueError("No ITEM IDs found in the file.")
    except FileNotFoundError:
        logging.error(f"File not found: {filename}")
        raise
    except IOError as e:
        logging.error(f"I/O error({e.errno}): {e.strerror}")
        raise
    return selected_item

def get_random_user_id(db_path: str) -> int:
    """
    Retrieves a random user_id from the SQLite database.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        int: A randomly selected user_id.

    Raises:
        ValueError: If no users are found in the database.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(rowid) FROM users;")
            max_rowid = cursor.fetchone()[0]
            if max_rowid is None:
                raise ValueError("No users found in the database.")
            random_rowid = random.randint(1, max_rowid)
            cursor.execute("SELECT user_id FROM users WHERE rowid >= ? LIMIT 1;", (random_rowid,))
            result = cursor.fetchone()
            if result is None:
                cursor.execute("SELECT user_id FROM users LIMIT 1;")
                result = cursor.fetchone()
            return result[0]
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
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
    item_id = select_random_item_id(items_file)
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
        event = generate_random_add_to_cart_event(MOVIES_FILE_PATH, ITEM_IDS_PATH)
        logging.info(f"Generated event: {event()}")
    except Exception as e:
        logging.error(f"Failed to generate add-to-cart event: {e}")

if __name__ == "__main__":
    main()
