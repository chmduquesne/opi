package main

import (
	"fmt"
	"os"

	"github.com/chmduquesne/opi"
)

const (
	usage = `Usage:
	archive <path> <id>
	restore <id> <path>
	`
)

func main() {
	if len(os.Args) != 3 {
		fmt.Print(usage)
		os.Exit(1)
	}
	if os.Args[1] == "archive" {
		opi.Archive(os.Args[2])
	}
}
