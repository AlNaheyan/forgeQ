.PHONY: build compose-down compose-up fmt test

build:
	go build ./...

test:
	go test ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

compose-up:
	docker compose up -d postgres redis

compose-down:
	docker compose down

