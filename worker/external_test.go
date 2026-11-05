package worker_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestExternalModuleImportsPublicWorker(t *testing.T) {
	dir := t.TempDir()
	current, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(current)
	module := fmt.Sprintf("module example.com/forgeq-consumer\n\ngo 1.26.10\n\nrequire github.com/AlNaheyan/forgeQ v0.0.0\n\nreplace github.com/AlNaheyan/forgeQ => %q\n", filepath.ToSlash(root))
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	source := `package example
import (
 "context"
 "encoding/json"
 "github.com/AlNaheyan/forgeQ/worker"
)
var Task = worker.Task{Name:"app.example", Handle:func(context.Context,json.RawMessage)(any,error){return "ok",nil}}
`
	if err := os.WriteFile(filepath.Join(dir, "example.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=mod", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOMAXPROCS=2", "GOFLAGS=-p=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("external consumer did not compile: %v\n%s", err, out)
	}
}
