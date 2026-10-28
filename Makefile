.PHONY: build compose-down compose-up fmt test test-integration test-recovery chaos-worker-crash chaos-redis-restart chaos-coordinator-restart

build:
	go build ./...

test:
	go test ./...

# Starts only disposable PostgreSQL/Redis, then removes that test stack.
test-recovery:
	bash scripts/test-recovery.sh

# Includes isolated backup/restore, upgrade, retention, and recovery assertions.
test-operations: test-recovery

test-integration:
	FORGEQUEUE_TEST_DATABASE_URL='postgres://forgequeue:forgequeue@localhost:55432/forgequeue?sslmode=disable' \
	FORGEQUEUE_TEST_REDIS_ADDR='localhost:6379' \
	go test -count=1 ./internal/api ./internal/coordinator ./internal/idempotency ./internal/queue ./internal/repository/postgres ./internal/worker

chaos-worker-crash:
	bash scripts/chaos-worker-crash.sh

chaos-redis-restart:
	bash scripts/chaos-restart-service.sh redis

chaos-coordinator-restart:
	bash scripts/chaos-restart-service.sh coordinator

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

compose-up:
	docker compose up -d postgres redis

compose-down:
	docker compose down
