import os
from dotenv import load_dotenv
import pg8000.native


load_dotenv(override=True)


def connect_to_db():
    return pg8000.native.Connection(
        user=os.getenv("PG_USER"), 
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )


def close_db_connection(conn):
    conn.close()
