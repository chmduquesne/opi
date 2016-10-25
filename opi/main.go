package main

import (
	"flag"
	"fmt"
)

const (
	usage = `Usage:
	archive <path> <id>
	restore <id> <path>
	`
)

func main() {
	if len(flag.Args()) != 3 {
		fmt.Print(usage)
	}
}
