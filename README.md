# Temporal local stack

Based on Temporal's official `samples-server/compose/docker-compose-postgres.yml` sample; uses Temporal Server 1.32.2, Admin Tools 1.32.2, UI 2.56.1, PostgreSQL 16.

Start:

```sh
docker compose up -d
```

Web UI: http://localhost:8082
Python SDK / Temporal gRPC endpoint: localhost:7233

Stop (keep DB):

```sh
docker compose down
```

Delete everything, **including workflow database**:

```sh
docker compose down -v
```

No RabbitMQ, Elasticsearch or app worker is included. Temporal uses PostgreSQL for persistence and visibility. Deploy your own Python worker separately to execute workflows and activities.

Development only: default credentials, no TLS or authentication. Temporal's Go server runs all four roles in one container.

## Namespaces

A namespace is an isolation boundary inside one Temporal server, much like a database inside one Postgres server: one server, several namespaces, and a client connects to exactly one (`Client.connect(..., namespace=...)`). A fresh server has none, so one must exist before any workflow can run.

On startup, `scripts/create-namespace.sh` runs once in a one-off admin-tools container. It waits for the server to report healthy, then creates the `default` namespace unless it already exists:

```sh
temporal operator namespace create -n default --address "$ADDRESS" --retention 7d
```

`--retention 7d` keeps a workflow's history for 7 days after it **finishes**, then deletes it. Running or paused workflows are never deleted. Anything you need to keep long term (e.g. review decisions) belongs in your own database, written by a step.

What a namespace separates:

- **Names**: workflow ids and task queues are unique per namespace. `review-dj-1` in `staging` and in `prod` are unrelated, and a worker polling a queue in one namespace never gets work from another.
- **Visibility**: the UI and list/search APIs show one namespace at a time.
- **Settings**: retention, archival and similar options are set per namespace.
- **Access**: with Temporal Cloud or self-hosted auth, permissions and credentials are granted per namespace.
- **Limits**: in Temporal Cloud, rate limits and billing are per namespace.

Usual split: one namespace per environment (`review-dev`, `review-staging`, `review-prod`), or per team or product when several share a server. Namespaces are not for separating workflow types inside one app; use task queues for that.

Locally, `default` is fine. Elsewhere, set `TEMPORAL_NAMESPACE` to the same value for the worker, whatever starts the workflows (e.g. the cron job) and the external embedding app: they only see each other's workflows inside one namespace.

## Review workflow: where things run

### General setup

The Temporal server only stores workflow state; your code runs in workers and in the external embedding app.

```
                 ┌───────────────────────────┐
  your app ─────►│  Temporal server          │◄──── UI (ops / you)
  (starts        │  + its own Postgres       │
   reviews)      │  stores workflow history  │
                 └─────▲─────────────────▲───┘
                       │ outbound gRPC   │ completes paused embed steps
                       │ (polling)       │
                 ┌─────┴──────────┐  ┌───┴──────────────────┐
                 │ Workers        │  │ External embedding   │
                 │ steps +        │  │ app                  │
                 │ workflow code  │  │                      │
                 └──┬───┬───┬─────┘  └───┬──────────────────┘
                    │   │   │            │
     taxonomy  ◄────┘   │   └──► LLM     │
     lookup service     ▼       (Jev)    ▼
                   embeddings store (Postgres table or S3) ◄─┘
```

- **Temporal server**: workflow history, timers, retries, paused steps. Never runs your code, needs none of your credentials.
- **Workers**: run every step, so they hold all step credentials (embeddings store, taxonomy service, LLM, product DB) as env vars or secrets. They connect out to Temporal; no inbound ports.
- **External embedding app**: writes embeddings to the store and completes the paused `embed` steps, so it needs the store and a Temporal client.
- Never put credentials in workflow inputs or step results: they are stored in Temporal history and shown in the UI.

### Django scenario

The worker is a Django management command in the same image as the web app, run as its own process. A cron job starts reviews in batches.

```
  cron ──► manage.py start_reviews          (same Docker image, separate processes)
           1. ORM: pick a batch of products
           2. start_workflow per product,
              id = review-<product_id>
           3. exit
                    │
                    ▼
        ┌───────────────────────────┐
        │  Temporal server          │◄──────────────────────────┐
        │  + its own Postgres       │                           │ completes paused
        └─────────────▲─────────────┘                           │ embed steps
                      │ polls task queue "review-django"        │
        ┌─────────────┴──────────────────────┐        ┌─────────┴────────────┐
        │ manage.py temporal_worker          │        │ External embedding   │
        │  workflow: flow only, no Django    │        │ app                  │
        │  steps:                            │        └───────────────────┬──┘
        │   product_data, product_content ───┼──► Django DB               │
        │     (sync, ORM, thread pool)       │    (Product)               │
        │   embed ───────────────────────────┼──► embeddings store ◄──────┘
        │   lookup_taxonomy ─────────────────┼──► taxonomy service
        │   ask_jev ─────────────────────────┼──► LLM
        └────────────────────────────────────┘
```

- **`start_reviews`** only starts workflows and exits. A fixed workflow id per product means an overlapping cron run cannot start a second review while one is still going.
- **`temporal_worker`** runs the steps. ORM steps are plain `def` functions run in a thread pool and call `close_old_connections()`, as Celery tasks do.
- **Workflow code** never touches Django or the ORM: it is replayed from history and must behave the same every time.
