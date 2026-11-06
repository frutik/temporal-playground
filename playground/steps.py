"""Activities (steps) of the review flow, with stubs, and the embeddings store.

embed is completed by an external application: the activity keys the text by md5,
creates an empty record if none exists, and returns the stored embedding if it is
there. Otherwise it parks its task token on the record and suspends.

Contract for the external application, per empty record (embedding IS NULL):
    1. UPDATE embeddings SET embedding = <json vector> WHERE hash = ? RETURNING waiters
    2. for each base64 token in waiters: complete that async activity with the vector
       (Temporal client: get_async_activity_handle(task_token=...).complete(vector));
       NOT_FOUND means the activity was retried or the workflow is gone - ignore it
"""

import base64
import hashlib
import json
import os
import sqlite3

from temporalio import activity

# shortcut: SQLite stands in for the shared DB / S3 bucket; worker and the external app must share this path
DB_PATH = os.environ.get("EMBEDDINGS_DB", "embeddings.sqlite")
DECISIONS = ("switch", "keep", "human", "reclassify")


@activity.defn
async def product_data(product_id: str) -> dict:
    return {"id": product_id, "title": f"Product {product_id}", "category": "Laptops"}


@activity.defn
async def known_candidates(product_id: str) -> list[str]:
    return ["Laptops", "Notebooks"]


@activity.defn
async def product_content(product_id: str) -> str:
    return f"Description text of product {product_id}"


@activity.defn
async def hallucinate_category(product_id: str) -> str:
    return "Portable computers"


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=30)  # autocommit: each statement is atomic
    conn.execute(
        "CREATE TABLE IF NOT EXISTS embeddings ("
        " hash TEXT PRIMARY KEY, text TEXT NOT NULL, embedding TEXT, waiters TEXT NOT NULL DEFAULT '[]')"
    )
    return conn


@activity.defn
async def embed(text: str) -> list[float]:
    key = hashlib.md5(text.encode()).hexdigest()
    token = base64.b64encode(activity.info().task_token).decode()
    conn = db()
    try:
        # one atomic upsert: create the empty record, or park our token unless the embedding is already there
        (embedding,) = conn.execute(
            "INSERT INTO embeddings (hash, text, waiters) VALUES (?, ?, json_array(?)) "
            "ON CONFLICT (hash) DO UPDATE SET waiters = CASE WHEN embedding IS NULL "
            " THEN json_insert(waiters, '$[#]', excluded.waiters ->> '$[0]') ELSE waiters END "
            "RETURNING embedding",
            (key, text, token),
        ).fetchone()
    finally:
        conn.close()
    if embedding is not None:
        return json.loads(embedding)
    activity.raise_complete_async()  # suspended until the external app completes this token


@activity.defn
async def lookup_taxonomy(vector: list[float]) -> list[str]:
    # demo: fail the first LOOKUP_FAILS attempts to watch Temporal's retries
    if activity.info().attempt <= int(os.environ.get("LOOKUP_FAILS", "0")):
        raise RuntimeError(f"taxonomy lookup unavailable (attempt {activity.info().attempt})")
    return ["Laptops", "Gaming laptops"] if vector[0] > 20 else ["Tablets"]


@activity.defn
async def merge(lists: list[list[str]]) -> list[str]:
    return list(dict.fromkeys(c for cs in lists for c in cs))


@activity.defn
async def ask_jev(req: dict) -> dict:
    return {"answer": req["stub_answer"], "category": req["candidates"][-1]}


@activity.defn
async def decide(verdict: dict) -> dict:
    if verdict["answer"] not in DECISIONS:
        return {**verdict, "decision": "human"}  # unknown judge output goes to a person
    return {**verdict, "decision": verdict["answer"]}


@activity.defn
async def switch_category(verdict: dict) -> str:
    return f"switched to {verdict['category']}"


@activity.defn
async def record_decision(verdict: dict) -> str:
    return f"recorded {verdict['decision']}"


@activity.defn
async def ask_human(verdict: dict) -> str:
    return "queued for human review"


ACTIVITIES = [
    product_data, known_candidates, product_content, hallucinate_category, embed,
    lookup_taxonomy, merge, ask_jev, decide, switch_category, record_decision, ask_human,
]
