"""Review/reclassify flow as a Temporal workflow, with stub activities.

    hallucinated_candidates = hallucinate_category >> embed >> lookup_taxonomy
    content_candidates      = product_content >> embed >> lookup_taxonomy
    reclassify_candidates   = [known_candidates, content_candidates, hallucinated_candidates] >> merge
    reclassify = record_decision >> [product_data, reclassify_candidates] >> ask_jev >> decide
                 >> branch(switch=switch_category, keep=record_decision, human=ask_human, reclassify=record_decision)
    review     = [product_data, known_candidates] >> ask_jev >> decide
                 >> branch(switch=switch_category, keep=record_decision, human=ask_human, reclassify=reclassify)

embed is completed by an external application: the activity keys the text by md5,
creates an empty record if none exists, and returns the stored embedding if it is
there. Otherwise it parks its task token on the record and suspends.

Contract for the external application, per empty record (embedding IS NULL):
    1. UPDATE embeddings SET embedding = <json vector> WHERE hash = ? RETURNING waiters
    2. for each base64 token in waiters: complete that async activity with the vector
       (Temporal client: get_async_activity_handle(task_token=...).complete(vector));
       NOT_FOUND means the activity was retried or the workflow is gone - ignore it

Modes (Temporal server on localhost:7233):
    (no args)  self-check: worker + a fake external application, temporary store
    worker     run the worker only
"""

import asyncio
import base64
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError
from temporalio.worker import Worker

TASK_QUEUE = "review"
# shortcut: SQLite stands in for the shared DB / S3 bucket; worker and the external app must share this path
DB_PATH = os.environ.get("EMBEDDINGS_DB", "embeddings.sqlite")
DECISIONS = ("switch", "keep", "human", "reclassify")


@dataclass
class ReviewInput:
    product_id: str
    # shortcut: stub judge answers, one per ask_jev round; replace with a real LLM call in ask_jev
    jev_answers: list[str] = field(default_factory=list)


# ---- stub activities -------------------------------------------------------


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


# ---- workflow ----------------------------------------------------------------


def run(fn, arg, timeout=timedelta(seconds=30)):
    return workflow.execute_activity(fn, arg, start_to_close_timeout=timeout)


@workflow.defn
class ReviewWorkflow:
    @workflow.run
    async def run(self, inp: ReviewInput) -> list[str]:
        self.inp, self.round, self.trail = inp, 0, []
        product, candidates = await asyncio.gather(
            run(product_data, inp.product_id), run(known_candidates, inp.product_id)
        )
        verdict = await self.judge(product, candidates)
        if verdict["decision"] == "reclassify":
            await self.reclassify(verdict)
        else:
            await self.branch(verdict)
        return self.trail

    async def reclassify(self, verdict: dict) -> None:
        self.trail.append(await run(record_decision, verdict))
        pid = self.inp.product_id
        product, candidates = await asyncio.gather(run(product_data, pid), self.reclassify_candidates(pid))
        verdict = await self.judge(product, candidates)
        # second round: no third one, a second 'reclassify' is only recorded
        await self.branch(verdict)

    async def reclassify_candidates(self, pid: str) -> list[str]:
        lists = await asyncio.gather(
            run(known_candidates, pid),
            self.embed_lookup(run(product_content, pid)),
            self.embed_lookup(run(hallucinate_category, pid)),
        )
        return await run(merge, list(lists))

    async def embed_lookup(self, text_step) -> list[str]:
        # long timeout: embed waits for the external batch; on timeout it retries and finds the stored result
        vector = await run(embed, await text_step, timeout=timedelta(days=1))
        return await run(lookup_taxonomy, vector)

    async def judge(self, product: dict, candidates: list[str]) -> dict:
        answer = self.inp.jev_answers[self.round] if self.round < len(self.inp.jev_answers) else "keep"
        self.round += 1
        verdict = await run(ask_jev, {"product": product, "candidates": candidates, "stub_answer": answer})
        return await run(decide, verdict)

    async def branch(self, verdict: dict) -> None:
        step = {"switch": switch_category, "human": ask_human}.get(verdict["decision"], record_decision)
        self.trail.append(await run(step, verdict))


# ---- worker + self-check ---------------------------------------------------


async def fake_external_app(client: Client) -> int:
    """Test stand-in for the external application: follows the contract in the module docstring."""
    conn = db()
    try:
        pending = conn.execute("SELECT hash, text FROM embeddings WHERE embedding IS NULL").fetchall()
        for key, text in pending:
            vector = [float(len(text)), 0.0, 1.0]
            # the row write serialises with embed's upsert: every waiter either sees the embedding or is returned here
            (waiters,) = conn.execute(
                "UPDATE embeddings SET embedding = ? WHERE hash = ? RETURNING waiters",
                (json.dumps(vector), key),
            ).fetchone()
            for token in json.loads(waiters):
                try:
                    await client.get_async_activity_handle(task_token=base64.b64decode(token)).complete(vector)
                except RPCError as e:
                    print(f"skip stale waiter on {key}: {e}")
    finally:
        conn.close()
    return len(pending)


def worker(client: Client) -> Worker:
    return Worker(client, task_queue=TASK_QUEUE, workflows=[ReviewWorkflow], activities=ACTIVITIES)


async def start(client: Client, answers: tuple[str, ...]):
    return await client.start_workflow(
        ReviewWorkflow.run,
        ReviewInput("p-1", list(answers)),
        id=f"review-{'-'.join(answers)}-{uuid.uuid4().hex[:6]}",
        task_queue=TASK_QUEUE,
    )


async def self_check(client: Client) -> None:
    global DB_PATH
    DB_PATH = os.path.join(tempfile.mkdtemp(), "embeddings.sqlite")
    count = lambda sql: db().execute(sql).fetchone()[0]  # noqa: E731
    async with worker(client):
        # 1. external app not run yet: the reclassify round must suspend on two empty records
        handle = await start(client, ("reclassify", "switch"))
        for _ in range(50):
            if count("SELECT count(*) FROM embeddings WHERE embedding IS NULL") == 2:
                break
            await asyncio.sleep(0.2)
        await asyncio.sleep(1)
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        assert count("SELECT count(*) FROM embeddings WHERE embedding IS NULL") == 2
        print("suspended on 2 empty records")
        # 2. the external app fills them and resumes the workflow
        assert await fake_external_app(client) == 2
        trail = await handle.result()
        assert trail == ["recorded reclassify", "switched to Tablets"], trail
        print(f"resumed -> {trail}")

        # 3. the rest; same texts, so the reclassify case is served from the store without waiting
        cases = {
            ("keep",): ["recorded keep"],
            ("switch",): ["switched to Notebooks"],
            ("bogus",): ["queued for human review"],
            ("reclassify", "reclassify"): ["recorded reclassify", "recorded reclassify"],
        }
        for answers, expected in cases.items():
            trail = await (await start(client, answers)).result()
            assert trail == expected, (answers, trail)
            print(f"{answers} -> {trail}")
        assert count("SELECT count(*) FROM embeddings") == 2  # cache hit: no new records


async def main() -> None:
    client = await Client.connect("localhost:7233")
    mode = sys.argv[1] if len(sys.argv) > 1 else "check"
    if mode == "worker":
        async with worker(client):
            await asyncio.Event().wait()
    else:
        await self_check(client)


if __name__ == "__main__":
    asyncio.run(main())
