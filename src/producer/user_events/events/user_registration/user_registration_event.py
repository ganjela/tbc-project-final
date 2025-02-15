import os
import random
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Union
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

USER_DB_PATH: str = os.getenv("USER_DB_PATH", "")

class UserRegistration:
    """
    Represents a user registration event with user details.
    """
    def __init__(self, user_id: int, age: int, email: str, preferred_language: str):
        self.user_id = user_id
        self.event_name = "consumer_registration"
        self.timestamp = datetime.now().isoformat()
        self.age = age
        self.masked_email = self.mask_email(email)
        self.preferred_language = preferred_language
    
    def __call__(self) -> Dict[str, Union[str, int]]:
        """
        Returns the event data as a dictionary.
        """
        return {
            "user_id": self.user_id,
            "event_name": self.event_name,
            "timestamp": self.timestamp,
            "age": self.age,
            "masked_email": self.masked_email,
            "preferred_language": self.preferred_language,
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
            "age": data.get("age"),
            "masked_email": data.get("masked_email"),
            "preferred_language": data.get("preferred_language"),
        }
    
    @staticmethod
    def mask_email(email: str) -> str:
        """
        Masks the email address for privacy.

        Args:
            email (str): The user's email address.

        Returns:
            str: Masked email address.
        """
        try:
            username, domain = email.split("@")
            return f"{username[0]}***@{domain}"
        except ValueError:
            logging.error("Invalid email format.")
            return "invalid_email"

def initialize_db(db_path: str) -> None:
    """
    Initializes the SQLite database by creating the 'users' table if it doesn't exist.

    Args:
        db_path (str): Path to the SQLite database file.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    age INTEGER,
                    masked_email TEXT,
                    preferred_language TEXT,
                    timestamp TEXT
                )
            """)
            logging.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"Database initialization failed: {e}")

def is_user_id_unique(db_path: str, user_id: int) -> bool:
    """
    Checks if the given user_id is unique in the database.

    Args:
        db_path (str): Path to the SQLite database file.
        user_id (int): The user ID to check.

    Returns:
        bool: True if the user_id is unique, False otherwise.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()[0]
            return result == 0
    except sqlite3.Error as e:
        logging.error(f"Error checking user ID uniqueness: {e}")
        return False

def store_user(db_path: str, user: UserRegistration) -> bool:
    """
    Stores the user registration event in the database if the user_id is unique.

    Args:
        db_path (str): Path to the SQLite database file.
        user (UserRegistration): The user registration event to store.

    Returns:
        bool: True if the user was stored successfully, False otherwise.
    """
    if is_user_id_unique(db_path, user.user_id):
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("""
                    INSERT INTO users (user_id, age, masked_email, preferred_language, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (user.user_id, user.age, user.masked_email, user.preferred_language, user.timestamp))
                logging.info(f"User {user.user_id} stored successfully.")
                return True
        except sqlite3.Error as e:
            logging.error(f"Error storing user: {e}")
            return False
    else:
        logging.warning(f"User ID {user.user_id} already exists.")
        return False

def generate_unique_user_id(db_path: str) -> int:
    """
    Generates a unique user ID that doesn't exist in the database.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        int: A unique user ID.
    """
    while True:
        user_id = random.randint(1, 100000)
        if is_user_id_unique(db_path, user_id):
            return user_id

def generate_random_registration(db_path: str) -> UserRegistration:
    """
    Generates a random user registration event and stores it in the database.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        UserRegistration: The generated user registration event.
    """
    user_id = generate_unique_user_id(db_path)
    age = random.randint(18, 70)
    email = f"user{user_id}@example.com"
    preferred_language = random.choice(["English", "Spanish", "French", "German", "Chinese"])
    
    new_user = UserRegistration(user_id, age, email, preferred_language)
    if store_user(db_path, new_user):
        return new_user
    else:
        raise Exception("Failed to store the new user registration.")

def main():
    """
    Main function to initialize the database and generate a random user registration.
    """
    if not USER_DB_PATH:
        logging.error("Environment variable USER_DB_PATH is not set.")
        return

    initialize_db(USER_DB_PATH)
    
    try:
        new_user = generate_random_registration(USER_DB_PATH)
        logging.info(f"Generated user registration: {new_user()}")
    except Exception as e:
        logging.error(f"Failed to generate user registration: {e}")
 
