## This script is used to drop the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("CONNECTION_STRING")

def drop_tables():
    conn = psycopg2.connect(CONNECTION)
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS segment CASCADE;")
        cur.execute("DROP TABLE IF EXISTS podcast CASCADE;")
        conn.commit()
        print("Tables dropped successfully")
    except Exception as e:
        print(f"Error dropping tables: {e}\n")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    drop_tables()