"""Review/reclassify flow as a Temporal workflow, with stub activities.

    hallucinated_candidates = hallucinate_category >> embed >> lookup_taxonomy
    content_candidates      = product_content >> embed >> lookup_taxonomy
    reclassify_candidates   = [known_candidates, content_candidates, hallucinated_candidates] >> merge
    reclassify = record_decision >> [product_data, reclassify_candidates] >> ask_jev >> decide
                 >> branch(switch=switch_category, keep=record_decision, human=ask_human, reclassify=record_decision)
    review     = [product_data, known_candidates] >> ask_jev >> decide
                 >> branch(switch=switch_category, keep=record_decision, human=ask_human, reclassify=reclassify)

Runs a worker and the self-check against a Temporal server on localhost:7233.
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

TASK_QUEUE = "review"
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


@activity.defn
async def embed(text: str) -> list[float]:
    return [float(len(text)), 0.0, 1.0]


@activity.defn
async def lookup_taxonomy(vector: list[float]) -> list[str]:
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


def run(fn, arg):
    return workflow.execute_activity(fn, arg, start_to_close_timeout=timedelta(seconds=30))


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
        return await run(lookup_taxonomy, await run(embed, await text_step))

    async def judge(self, product: dict, candidates: list[str]) -> dict:
        answer = self.inp.jev_answers[self.round] if self.round < len(self.inp.jev_answers) else "keep"
        self.round += 1
        verdict = await run(ask_jev, {"product": product, "candidates": candidates, "stub_answer": answer})
        return await run(decide, verdict)

    async def branch(self, verdict: dict) -> None:
        step = {"switch": switch_category, "human": ask_human}.get(verdict["decision"], record_decision)
        self.trail.append(await run(step, verdict))


# ---- worker + self-check ---------------------------------------------------


async def main() -> None:
    client = await Client.connect("localhost:7233")
    async with Worker(client, task_queue=TASK_QUEUE, workflows=[ReviewWorkflow], activities=ACTIVITIES):
        cases = {
            ("keep",): ["recorded keep"],
            ("switch",): ["switched to Notebooks"],
            ("bogus",): ["queued for human review"],
            ("reclassify", "switch"): ["recorded reclassify", "switched to Tablets"],
            ("reclassify", "reclassify"): ["recorded reclassify", "recorded reclassify"],
        }
        for answers, expected in cases.items():
            trail = await client.execute_workflow(
                ReviewWorkflow.run,
                ReviewInput("p-1", list(answers)),
                id=f"review-{'-'.join(answers)}-{uuid.uuid4().hex[:6]}",
                task_queue=TASK_QUEUE,
            )
            assert trail == expected, (answers, trail)
            print(f"{answers} -> {trail}")


if __name__ == "__main__":
    asyncio.run(main())
