.PHONY: build compose-down compose-up fmt test test-integration

build:
	go build ./...

test:
	go test ./...

test-integration:
	FORGEQUEUE_TEST_DATABASE_URL='postgres://forgequeue:forgequeue@localhost:55432/forgequeue?sslmode=disable' \
	FORGEQUEUE_TEST_REDIS_ADDR='localhost:6379' \
	go test -count=1 ./internal/api ./internal/queue ./internal/repository/postgres

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

compose-up:
	docker compose up -d postgres redis

compose-down:
	docker compose down
