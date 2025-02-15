import sqlite3
import random
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def get_random_cart_id(db_path: str) -> int:
    """
    Retrieves a random cart_id from the SQLite database.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        int: A randomly selected cart_id.

    Raises:
        ValueError: If no cart_events are found in the database.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(rowid) FROM cart_events;")
            max_rowid = cursor.fetchone()[0]
            if max_rowid is None:
                raise ValueError("No cart_events found in the database.")
            random_rowid = random.randint(1, max_rowid)
            cursor.execute("SELECT cart_id FROM cart_events WHERE rowid >= ? LIMIT 1;", (random_rowid,))
            result = cursor.fetchone()
            if result is None:
                cursor.execute("SELECT cart_id FROM cart_events LIMIT 1;")
                result = cursor.fetchone()
            return result[0]
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        raise