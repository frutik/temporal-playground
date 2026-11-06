"""Worker and self-check for the review workflow.

Modes (Temporal server on localhost:7233):
    (no args)  self-check: worker + a fake external application, temporary store
    worker     run the worker only
"""

import asyncio
import base64
import json
import os
import sys
import tempfile
import uuid

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError
from temporalio.worker import Worker

import steps
from workflow import ReviewInput, ReviewWorkflow

TASK_QUEUE = "review"


async def fake_external_app(client: Client) -> int:
    """Test stand-in for the external application: follows the contract in the steps module docstring."""
    conn = steps.db()
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
    return Worker(client, task_queue=TASK_QUEUE, workflows=[ReviewWorkflow], activities=steps.ACTIVITIES)


async def start(client: Client, answers: tuple[str, ...]):
    return await client.start_workflow(
        ReviewWorkflow.run,
        ReviewInput("p-1", list(answers)),
        id=f"review-{'-'.join(answers)}-{uuid.uuid4().hex[:6]}",
        task_queue=TASK_QUEUE,
    )


async def self_check(client: Client) -> None:
    steps.DB_PATH = os.path.join(tempfile.mkdtemp(), "embeddings.sqlite")
    count = lambda sql: steps.db().execute(sql).fetchone()[0]  # noqa: E731
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
