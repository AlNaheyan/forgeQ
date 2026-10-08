# ForgeQueue

ForgeQueue is a production-style distributed job execution platform written in
Go. It is being built to make coordination, failure recovery, at-least-once
delivery, idempotency, backpressure, and observability explicit rather than
hiding them behind a workflow framework.

The project is under active development. Wave 0 establishes the shared service
foundation and local PostgreSQL/Redis dependencies; the durable job API arrives
in Wave 1.

## Planned architecture

```text
Client --HTTP--> API --+--> PostgreSQL (durable truth)
                       |
                       +--> Redis (queue hints and coordination)
                                  |
Worker --gRPC--> Coordinator -----+
```

ForgeQueue will provide at-least-once execution. A job may run more than once
when a worker completes its side effect but fails before acknowledging the
result, so side-effecting handlers must be idempotent.

## Requirements

- Go 1.23 or newer
- Docker with Docker Compose

## Local development

Copy the example environment if you want to override the development defaults:

```bash
cp .env.example .env
```

Start PostgreSQL and Redis:

```bash
docker compose up -d postgres redis
```

Run the test suite:

```bash
go test ./...
```

Build every command:

```bash
go build ./...
```

Stop the local dependencies:

```bash
docker compose down
```

## Repository map

- `cmd/`: service and tooling entry points
- `internal/config/`: typed environment configuration
- `internal/telemetry/`: shared structured logging and, later, metrics
- `deployments/`: local deployment definitions
- `docs/`: build specification and architecture documentation

The implementation follows the ordered waves in
[`docs/forgequeue_build_spec.md`](docs/forgequeue_build_spec.md).

