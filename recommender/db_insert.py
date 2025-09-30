## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
# from datasets import load_dataset
import pandas as pd

import glob
from typing import Dict, Any, Iterable, List

from utils import fast_pg_insert

load_dotenv()

CONNECTION = os.environ.get("CONNECTION_STRING")

SEGMENT_COLS = ["id", "podcast_id", "start_time", "end_time", "content", "embedding"]
PODCAST_COLS = ["id","title"]
DOCS_GLOB = os.path.join("documents", "batch_request_*.jsonl")
EMB_GLOB  = os.path.join("embedding", "*.jsonl")

def iter_jsonl(pattern: str) -> Iterable[Dict[str, Any]]:
    for path in sorted(glob.glob(pattern)):
        try:
            jsonl_file = open(path, "r", encoding="utf-8")
            for line in jsonl_file:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    yield json.loads(line)
                except Exception:
                    continue
        finally:
            jsonl_file.close()
            
# TODO: Read documents files
def get_doc_lookup_and_podcast_table():
    document_lookup: Dict[str, Dict[str, Any]] = {}
    podcast_rows: Dict[str, str] = {}

    for obj in iter_jsonl(DOCS_GLOB):
        segment_id = obj.get("custom_id")
        if not segment_id:
            continue

        body = obj.get("body") or {}
        content = body.get("input")            
        metadata = (body.get("metadata") or {})
        podcast_id = metadata.get("podcast_id")
        title = metadata.get("title")
        start_time = metadata.get("start_time")
        end_time = metadata.get("stop_time")

        document_lookup[segment_id] = {
            "podcast_id": podcast_id,
            "content": content,
            "start_time": float(start_time),
            "end_time": float(end_time),
        }

        if podcast_id and podcast_id not in podcast_rows:
            podcast_rows[podcast_id] = title

    podcasts_df = pd.DataFrame(
        [{"id": pid, "title": title} for pid, title in podcast_rows.items()]
    )
    return document_lookup, podcasts_df

# TODO: Read the embedding files
def flush_segments(buffer: List[Dict[str, Any]], connection: str) -> int:
    if not buffer:
        return 0
    
    df = pd.DataFrame(buffer, columns=SEGMENT_COLS)
    fast_pg_insert(df, connection, "segment", SEGMENT_COLS)
    
    buffer.clear()
    return len(df)

def to_pgvector_literal(vec: List[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in vec) + "]"

def stream_insert_segments(
    docs_lookup: Dict[str, Dict[str, Any]],
    connection: str,
    chunk_size: int = 200_000
):
    buffer: List[Dict[str, Any]] = []
    total = 0

    for obj in iter_jsonl(EMB_GLOB):
        segment_id = obj.get("custom_id")
        if not segment_id:
            continue

        doc = docs_lookup.get(segment_id)
        if not doc:
            continue

        resp = obj.get("response") or {}
        body = resp.get("body") or {}
        data = body.get("data") or []
        if not data:
            continue
        emb = data[0].get("embedding")
        if not emb:
            continue

        buffer.append({
            "id": segment_id,
            "podcast_id": doc["podcast_id"],
            "start_time": doc["start_time"],
            "end_time": doc["end_time"],
            "content": doc["content"],
            "embedding": to_pgvector_literal(emb),
        })

        if len(buffer) >= chunk_size:
            total += flush_segments(buffer, connection)

    total += flush_segments(buffer, connection)
    print(f"Inserted total segments: {total}")

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
# ds = load_dataset("Whispering-GPT/lex-fridman-podcast")

# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time
def insert_into_tables():
    docs_lookup, podcasts_df = get_doc_lookup_and_podcast_table()
    fast_pg_insert(podcasts_df, CONNECTION, "podcast", PODCAST_COLS)
    print(f"Inserted podcasts: {len(podcasts_df)}")
    
    stream_insert_segments(docs_lookup, CONNECTION, chunk_size=200000)

if __name__ == "__main__":
    insert_into_tables()
