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

func OpiServed() func() {
	cmd := exec.Command("opi-serve")
	err := cmd.Start()
	if err != nil {
		fmt.Errorf("%v", err)
		return func() {}
	}
	return func() {
		err := cmd.Process.Signal(os.Kill)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Print(usage)
		os.Exit(1)
	}

	defer OpiServed()()

	if os.Args[1] == "archive" {
		opi.Archive(os.Args[2])
	}

}
