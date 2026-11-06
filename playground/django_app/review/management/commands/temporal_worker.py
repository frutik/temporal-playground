import asyncio
from concurrent.futures import ThreadPoolExecutor

from django.conf import settings
from django.core.management.base import BaseCommand
from temporalio.client import Client
from temporalio.worker import Worker

from review.steps import ACTIVITIES
from workflow import ReviewWorkflow


class Command(BaseCommand):
    help = "Run the Temporal worker: polls the review task queue and runs workflows and steps."

    def handle(self, *args, **options):
        asyncio.run(self.run())

    async def run(self):
        client = await Client.connect(settings.TEMPORAL_ADDRESS, namespace=settings.TEMPORAL_NAMESPACE)
        # threads for the sync (ORM) steps; async steps still run on the event loop
        with ThreadPoolExecutor(max_workers=10) as pool:
            worker = Worker(
                client,
                task_queue=settings.TEMPORAL_TASK_QUEUE,
                workflows=[ReviewWorkflow],
                activities=ACTIVITIES,
                activity_executor=pool,
            )
            self.stdout.write(f"worker polling {settings.TEMPORAL_TASK_QUEUE!r} on {settings.TEMPORAL_ADDRESS}")
            await worker.run()
