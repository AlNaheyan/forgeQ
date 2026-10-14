# Worker coordinator protocol

`worker.proto` defines the versioned internal gRPC contract between workers and
the coordinator. Generated Go sources are checked into `workerpb/` so ordinary
builds do not require the protobuf compiler.

To regenerate them, install `protoc` and the pinned Go plugins:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```

Ensure the directory containing those binaries is on `PATH`, then run:

```sh
./proto/generate.sh
```
