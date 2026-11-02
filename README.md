# ForgeQ

A distributed job execution platform built with Go, PostgreSQL, Redis, and
gRPC. ForgeQ provides tenant-authenticated job submission, bounded-concurrency
workers, retries, and lease-based recovery, with an embedded dashboard to launch
jobs and watch them run.

## Requirements

- Go 1.23 or newer
- Docker with Docker Compose
- OpenSSL for generating a local service credential

![ForgeQ system design](assets/forgeq-system-design.png)

## Local development

From the repository root, configure your environment and start the dependencies:

```bash
cp .env.example .env
export CONTROL_PLANE_TOKEN="$(openssl rand -hex 32)"
export DATABASE_URL='postgres://forgequeue:forgequeue@localhost:55432/forgequeue?sslmode=disable'
docker compose up -d --wait postgres redis
```

Keep the service token for subsequent runs by saving it as `CONTROL_PLANE_TOKEN`
in your local `.env`. Go commands do not automatically load that file.

Create a tenant and save the returned `api_key` securely:

```bash
go run ./cmd/tenant create --name local --operator
```

Build and start the API, coordinator, and worker in the same terminal:

```bash
docker compose up -d --build api coordinator worker
```

Open [localhost:8080/dashboard](http://localhost:8080/dashboard), connect with
your API key, and launch a job. Local development uses HTTP; public deployment
requires HTTPS termination.

Run tests or stop the stack:

```bash
go test ./...
make test-recovery       # Integration tests using disposable PostgreSQL/Redis
docker compose down     # Stops the stack without deleting its data volumes
```

For optional monitoring, Kafka, and deployment guidance, see the
[operations guide](docs/operations.md) and [event-stream setup](docs/event-stream.md).
