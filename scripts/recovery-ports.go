//go:build ignore

// Select two distinct loopback ports before Compose binds them explicitly.
// Publishing port 0 in Docker can choose a new host port on container restart.
package main

import (
	"fmt"
	"net"
)

func main() {
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer first.Close()
	second, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer second.Close()
	fmt.Println(first.Addr().(*net.TCPAddr).Port, second.Addr().(*net.TCPAddr).Port)
}
