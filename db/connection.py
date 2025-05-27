# import os
from pg8000.native import Connection
from dotenv import load_dotenv
import os

# Create a connect_to_db function to return a database connection object.

def connect_to_db(dotenv_path=".env.dev"):
    try:
        load_dotenv(dotenv_path=dotenv_path)
        return Connection(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            database=os.getenv("PG_DATABASE"),
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT"))
        )
    except Exception:
        print("Error connecting to database.")

# Create a close_db function that closes a passed database connection object.

def close_db(db):
    try:
        db.close()
    except Exception:
        print("Error closing database connection.")