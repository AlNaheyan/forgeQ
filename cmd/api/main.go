package main

import (
	"fmt"
	"os"

	"forgequeue/internal/app"
)

func main() {
	if err := app.Run("api", os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
