## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

CONNECTION = os.environ.get("CONNECTION_STRING")

def run(cur, title, sql, params=()):
    print(f"\n=== {title} ===")
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()

        if not rows:
            print("No Rows\n")
            return

        headers = [d.name for d in cur.description]
        print(tabulate(rows, headers=headers, tablefmt="psql", floatfmt=".6f"))
        print()
    except Exception as e:
        print(f"Error query tables: {e}\n")

def query_tables():
    conn = psycopg2.connect(CONNECTION)
    cur = conn.cursor()

    seg_similar = """
        SELECT p.title AS podcast_title,
            s2.id AS segment_id,
            s2.content,
            s2.start_time,
            s2.end_time,
            (s1.embedding <-> s2.embedding) AS distance
        FROM segment AS s1
        JOIN segment AS s2 ON s2.id <> s1.id
        JOIN podcast AS p ON p.id = s2.podcast_id
        WHERE s1.id = %s
        ORDER BY distance ASC
        LIMIT 5;
    """

    seg_dissimilar = """
        SELECT p.title AS podcast_title,
            s2.id AS segment_id,
            s2.content,
            s2.start_time,
            s2.end_time,
            (s1.embedding <-> s2.embedding) AS distance
        FROM segment AS s1
        JOIN segment AS s2 ON s2.id <> s1.id
        JOIN podcast AS p ON p.id = s2.podcast_id
        WHERE s1.id = %s
        ORDER BY distance DESC
        LIMIT 5;
    """

    seg_to_epi = """
        WITH episode_centroids AS (
            SELECT podcast_id, AVG(embedding) AS centroid
            FROM segment
            GROUP BY podcast_id
        ),
        query_segment AS (
            SELECT s.embedding, s.podcast_id
            FROM segment s
            WHERE s.id = %s
        )
        SELECT p.title,
            (qs.embedding <-> ec.centroid) AS distance
        FROM query_segment AS qs
        JOIN episode_centroids AS ec ON ec.podcast_id <> qs.podcast_id
        JOIN podcast AS p ON p.id = ec.podcast_id
        ORDER BY distance ASC
        LIMIT 5;
    """

    epi_to_epi = """
        WITH episode_centroids AS (
            SELECT podcast_id, AVG(embedding) AS centroid
            FROM segment
            GROUP BY podcast_id
        ),
        query_segment AS (
            SELECT ec.centroid
            FROM episode_centroids AS ec
            WHERE ec.podcast_id = %s
        )
        SELECT p.title,
            (qs.centroid <-> ec.centroid) AS distance
        FROM query_segment AS qs
        JOIN episode_centroids AS ec ON ec.podcast_id <> %s
        JOIN podcast AS p ON p.id = ec.podcast_id
        ORDER BY distance ASC
        LIMIT 5;
    """

    run(cur, "Q1: 5 most similar to 267:476", seg_similar, ("267:476",))
    run(cur, "Q2: 5 most dissimilar to 267:476", seg_dissimilar, ("267:476",))
    run(cur, "Q3: 5 most similar to 48:511", seg_similar, ("48:511",))
    run(cur, "Q4: 5 most similar to 51:56", seg_similar, ("51:56",))

    run(cur, "Q5a: 5 episodes similar to segment 267:476", seg_to_epi, ("267:476",))
    run(cur, "Q5b: 5 episodes similar to segment 48:511", seg_to_epi, ("48:511",))
    run(cur, "Q5c: 5 episodes similar to segment 51:56", seg_to_epi, ("51:56",))

    run(cur, "Q6: 5 episodes similar to episode VeH7qKZr0WI", epi_to_epi, ("VeH7qKZr0WI", "VeH7qKZr0WI"))
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    query_tables()