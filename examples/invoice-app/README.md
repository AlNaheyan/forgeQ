# External invoice application

This separate Go module uses ForgeQ's public `client` and `worker` packages.
Its `invoice.create` handler writes an invoice record to an application-owned
PostgreSQL schema. It does not send email or access an external payment service.
The included `replace` in `go.mod` points at the local ForgeQ checkout; use a
published ForgeQ version and remove that line in a separate project.

For a local demo, first follow the repository's main README to start PostgreSQL,
Redis, the coordinator and API, and to create a tenant API key. Keep the service
token and database URL exported in your shell. Add the application job type to
the API and apply the example schema to the **local development database**:

```sh
export FORGEQ_JOB_TYPES=invoice.create
docker compose up -d --no-deps --build api
docker compose exec -T postgres psql -U forgequeue -d forgequeue < examples/invoice-app/schema.sql
```

In a second terminal, start the application-owned worker. Use the same local
service token that the coordinator uses; this mode is development-only:

```sh
cd examples/invoice-app
export INVOICE_DATABASE_URL='postgres://forgequeue:forgequeue@localhost:55432/forgequeue?sslmode=disable'
export FORGEQ_COORDINATOR_ADDR='localhost:9090'
export FORGEQ_DEV_TOKEN='<your-local-service-token>'
export FORGEQ_ALLOW_INSECURE=true
go run . worker
```

In another terminal, submit an invoice and inspect the returned job ID:

```sh
cd examples/invoice-app
export FORGEQ_API_KEY='<your-tenant-api-key>'
export FORGEQ_ALLOW_INSECURE=true
export FORGEQ_IDEMPOTENCY_KEY='invoice-inv-001'
go run . submit inv-001 4500
go run . get '<returned-job-id>'
```

The default API endpoint is `http://localhost:8080`; override it with
`FORGEQ_API_URL`. For production use HTTPS and mTLS, set the worker's
`FORGEQ_TLS_CA_FILE`, `FORGEQ_TLS_CERT_FILE`, `FORGEQ_TLS_KEY_FILE`,
`FORGEQ_TLS_SERVER_NAME`, and `FORGEQ_WORKER_ID` to the assigned service identity.
Use a dedicated application database, verified TLS, a separate read/write role,
and a secure secret provider. The worker's role needs `USAGE` on `invoice_app`
and `SELECT`/`INSERT` on its table; only a migration role should create schema.

ForgeQ provides at-least-once execution. This example writes with a unique
tenant-and-operation key so a retry after an ambiguous acknowledgment does not
create a second invoice row. External effects such as sending email still need
their own idempotency strategy.
