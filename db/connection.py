# import os
from pg8000.native import Connection
from dotenv import load_dotenv
import os

""" Functions for creating and closing database connections """
load_dotenv()

def connect_to_db():
    try:
        
        db = Connection(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            database=os.getenv("PG_DATABASE"),
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT")),
        )
        return db
    except Exception:
        print("Error connecting to database.")


def close_db(db):
    try:
        db.close()
    except Exception:
        print("Error closing database connection.")

