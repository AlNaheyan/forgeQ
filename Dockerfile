ARG GO_IMAGE=golang:1.25.3-alpine@sha256:aee43c3ccbf24fdffb7295693b6e33b21e01baec1b2a55acc351fde345e9ec34
ARG RUNTIME_IMAGE=alpine:3.21@sha256:ee16d92f96db08d9d2d07da48555a27f3a69d8f0759021dbc601195ed0d2b5bd
FROM ${GO_IMAGE} AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG SERVICE=api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/forgequeue ./cmd/${SERVICE}

FROM ${RUNTIME_IMAGE}
ARG REVISION=development
ARG SERVICE=api
LABEL org.opencontainers.image.revision=$REVISION io.forgeq.service=$SERVICE
RUN addgroup -S forgequeue && adduser -S -G forgequeue forgequeue
COPY --from=build /out/forgequeue /usr/local/bin/forgequeue
USER forgequeue
ENTRYPOINT ["/usr/local/bin/forgequeue"]

