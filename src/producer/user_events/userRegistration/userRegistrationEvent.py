import sqlite3
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

# SQLite Database Setup
def initialize_db():
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            age INTEGER,
            masked_email TEXT,
            preferred_language TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

# Function to Check User ID Uniqueness
def is_user_id_unique(user_id: int) -> bool:
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()[0]
    conn.close()
    return result == 0

# Function to Store User in Database
def store_user(user: UserRegistration):
    if is_user_id_unique(user.user_id):
        conn = sqlite3.connect("users.db")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO users (user_id, age, masked_email, preferred_language, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (user.user_id, user.age, user.masked_email, user.preferred_language, user.timestamp))
        conn.commit()
        conn.close()
        return True
    return False

# Generate Unique Random User ID
def generate_unique_user_id():
    while True:
        user_id = random.randint(1, 100000)
        if is_user_id_unique(user_id):
            return user_id

# Generate Random User Registration
def generate_random_registration():
    user_id = generate_unique_user_id()
    age = random.randint(18, 70)
    masked_email = f"user{user_id}@example.com"
    preferred_language = random.choice(["English", "Spanish", "French", "German", "Chinese"])
    
    new_user = UserRegistration(user_id, age, masked_email, preferred_language)
    store_user(new_user)
    return new_user

