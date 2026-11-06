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
