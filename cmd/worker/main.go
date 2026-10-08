package main

import (
	"fmt"
	"os"

	"forgequeue/internal/app"
)

func main() {
	if err := app.Run("worker", os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
