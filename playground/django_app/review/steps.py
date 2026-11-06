"""Django-backed steps. Temporal finds activities by name, so these replace the playground stubs
of the same name while ReviewWorkflow stays unchanged."""

from django.db import close_old_connections
from temporalio import activity

import steps as stubs
from review.models import Product


# Sync `def`, not `async def`: the ORM is synchronous, so the worker runs these in a thread pool.
@activity.defn
def product_data(product_id: str) -> dict:
    close_old_connections()  # worker threads live outside the request cycle
    try:
        p = Product.objects.get(product_id=product_id)
        return {"id": p.product_id, "title": p.title, "category": p.category}
    finally:
        close_old_connections()


@activity.defn
def product_content(product_id: str) -> str:
    close_old_connections()
    try:
        return Product.objects.get(product_id=product_id).description
    finally:
        close_old_connections()


_overrides = [product_data, product_content]
ACTIVITIES = _overrides + [
    a for a in stubs.ACTIVITIES if a.__name__ not in {f.__name__ for f in _overrides}
]
