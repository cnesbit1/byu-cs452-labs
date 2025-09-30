## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("CONNECTION_STRING")
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector;"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
CREATE TABLE IF NOT EXISTS podcast (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL
);
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
CREATE TABLE IF NOT EXISTS segment (
    id TEXT PRIMARY KEY,
    podcast_id TEXT NOT NULL REFERENCES podcast(id) ON DELETE CASCADE ON UPDATE RESTRICT,
    start_time DOUBLE PRECISION,
    end_time DOUBLE PRECISION,
    content TEXT NOT NULL,
    embedding VECTOR(128) NOT NULL
);
"""

# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)
def create_tables():
    conn = psycopg2.connect(CONNECTION)
    cur = conn.cursor()
    try:
        cur.execute(CREATE_EXTENSION)
        cur.execute(CREATE_PODCAST_TABLE)
        cur.execute(CREATE_SEGMENT_TABLE)
        conn.commit()
        print("Tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}\n")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_tables()

