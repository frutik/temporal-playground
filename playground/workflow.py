"""Review/reclassify flow as a Temporal workflow. Flow logic only; side effects live in steps.

    hallucinated_candidates = hallucinate_category >> embed >> lookup_taxonomy
    content_candidates      = product_content >> embed >> lookup_taxonomy
    reclassify_candidates   = [known_candidates, content_candidates, hallucinated_candidates] >> merge
    reclassify = record_decision >> [product_data, reclassify_candidates] >> ask_jev >> decide
                 >> branch(switch=switch_category, keep=record_decision, human=ask_human, reclassify=record_decision)
    review     = [product_data, known_candidates] >> ask_jev >> decide
                 >> branch(switch=switch_category, keep=record_decision, human=ask_human, reclassify=reclassify)
"""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow

# steps imports sqlite3 etc.; pass it through so the workflow sandbox does not re-import it
with workflow.unsafe.imports_passed_through():
    from steps import (
        ask_human, ask_jev, decide, embed, hallucinate_category, known_candidates, lookup_taxonomy,
        merge, product_content, product_data, record_decision, switch_category,
    )


@dataclass
class ReviewInput:
    product_id: str
    # shortcut: stub judge answers, one per ask_jev round; replace with a real LLM call in ask_jev
    jev_answers: list[str] = field(default_factory=list)


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
            self.candidates_lookup(run(product_content, pid)),
            self.candidates_lookup(run(hallucinate_category, pid)),
        )
        return await run(merge, list(lists))

    async def candidates_lookup(self, text_step) -> list[str]:
        # long timeout: embed waits for the external app; on timeout it retries and finds the stored result
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
