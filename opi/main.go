package main

import (
	"fmt"
	"os"
	"os/exec"

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

	cmd := exec.Command("opi-serve")
	err := cmd.Start()
	if err != nil {
		fmt.Print(err)
	}

	if os.Args[1] == "archive" {
		opi.Archive(os.Args[2])
	}

	cmd.Process.Kill()
}
