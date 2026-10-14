FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG SERVICE=api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/forgequeue ./cmd/${SERVICE}

FROM alpine:3.21
RUN addgroup -S forgequeue && adduser -S -G forgequeue forgequeue
COPY --from=build /out/forgequeue /usr/local/bin/forgequeue
USER forgequeue
ENTRYPOINT ["/usr/local/bin/forgequeue"]

