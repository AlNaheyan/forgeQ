ARG GO_IMAGE=golang:1.27.3-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb
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

