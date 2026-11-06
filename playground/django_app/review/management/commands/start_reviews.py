import asyncio

from django.conf import settings
from django.core.management.base import BaseCommand
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from review.models import Product
from workflow import ReviewInput, ReviewWorkflow


class Command(BaseCommand):
    help = "Cron entry point: start one review workflow per product in a batch, then exit."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--jev", default="", help="stub judge answers, e.g. reclassify,switch")

    def handle(self, *args, limit, jev, **options):
        # ORM first, in sync code: Django refuses ORM calls from inside the event loop
        # shortcut: takes the first N products; put the real "needs review" filter here
        product_ids = list(Product.objects.order_by("id").values_list("product_id", flat=True)[:limit])
        answers = [a for a in jev.split(",") if a]
        started, skipped = asyncio.run(self.start_all(product_ids, answers))
        self.stdout.write(f"started {started}, already running {skipped}")

    async def start_all(self, product_ids: list[str], answers: list[str]) -> tuple[int, int]:
        client = await Client.connect(settings.TEMPORAL_ADDRESS, namespace=settings.TEMPORAL_NAMESPACE)
        started = skipped = 0
        for pid in product_ids:
            try:
                await client.start_workflow(
                    ReviewWorkflow.run,
                    ReviewInput(pid, answers),
                    # one id per product: if the last cron run's review is still going, Temporal refuses a second one
                    id=f"review-{pid}",
                    task_queue=settings.TEMPORAL_TASK_QUEUE,
                )
                started += 1
            except WorkflowAlreadyStartedError:
                skipped += 1
        return started, skipped
