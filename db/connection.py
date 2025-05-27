# import os
from pg8000.native import Connection
from dotenv import load_dotenv
import os


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


def close_db(db):
    try:
        db.close()
    except Exception:
        print("Error closing database connection.")
