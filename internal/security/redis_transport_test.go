package security

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/platform"
)

// A local RESP fixture verifies that the real Redis constructor sends credentials
// over verified TLS. It is not a production Redis deployment rehearsal.
func TestRedisConstructorAuthenticatesOverTLS(t *testing.T) {
	ca := makeCA(t)
	c, k := ca.issue(t, "", time.Now().Add(time.Hour))
	cert, err := Certificate(c, k)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveRESPFixture(conn)
		}
	}()
	cfg, err := RedisTLS(true, ca.path, "localhost", true)
	if err != nil {
		t.Fatal(err)
	}
	for _, password := range []string{"synthetic-test-password", "wrong-test-password", ""} {
		settings := platform.RedisSettings{Username: "fixture", Password: password, TLSConfig: cfg, PoolSize: 1, DialTimeout: time.Second, IOTimeout: time.Second, PingTimeout: time.Second}
		client, err := platform.OpenRedisWithSettings(context.Background(), listener.Addr().String(), settings)
		if password == "synthetic-test-password" {
			if err != nil {
				t.Fatal(err)
			}
			client.Close()
		} else if err == nil {
			client.Close()
			t.Fatal("Redis accepted invalid credentials")
		}
		if err != nil && password != "" && strings.Contains(err.Error(), password) {
			t.Fatal("Redis error leaked credentials")
		}
	}
	wrongCA := makeCA(t)
	wrong, _ := RedisTLS(true, wrongCA.path, "localhost", true)
	if client, err := platform.OpenRedisWithSettings(context.Background(), listener.Addr().String(), platform.RedisSettings{Password: "synthetic-test-password", TLSConfig: wrong, PoolSize: 1, DialTimeout: time.Second, IOTimeout: time.Second, PingTimeout: time.Second}); err == nil {
		client.Close()
		t.Fatal("Redis accepted untrusted CA")
	}
}
func serveRESPFixture(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	authenticated := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if !strings.HasPrefix(line, "*") {
			return
		}
		count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || count < 1 || count > 20 {
			return
		}
		args := make([]string, count)
		for i := range args {
			line, err = reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(line, "$") {
				return
			}
			size, e := strconv.Atoi(strings.TrimSpace(line[1:]))
			if e != nil || size < 0 || size > 1024 {
				return
			}
			b := make([]byte, size+2)
			if _, e = io.ReadFull(reader, b); e != nil {
				return
			}
			args[i] = string(b[:size])
		}
		reply := "+OK\r\n"
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			reply = "-ERR unknown command 'hello'\r\n"
		case "AUTH":
			if args[len(args)-1] == "synthetic-test-password" {
				authenticated = true
			} else {
				reply = "-WRONGPASS invalid credentials\r\n"
			}
		case "PING":
			if authenticated {
				reply = "+PONG\r\n"
			} else {
				reply = "-NOAUTH authentication required\r\n"
			}
		}
		if _, err = fmt.Fprint(conn, reply); err != nil {
			return
		}
	}
}
