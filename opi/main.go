package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/chmduquesne/opi"
)

const (
	usage = `Usage:
	archive <path> <id>
	restore <id> <path>
	`
)

func OpiServed() func() {
	fmt.Println("Starting opi-serve")
	cmd := exec.Command("opi-serve")
	err := cmd.Start()
	stop := func() {
		fmt.Println("Stopping opi-serve")
		if err != nil {
			fmt.Errorf("%v", err)
		} else {
			err = cmd.Process.Signal(os.Kill)
			if err != nil {
				fmt.Errorf("%v", err)
			}
		}
	}
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		stop()
		os.Exit(1)
	}()
	return stop
}

func main() {
	if len(os.Args) != 4 {
		fmt.Print(usage)
		os.Exit(1)
	}

	defer OpiServed()()

	if os.Args[1] == "archive" {
		s := opi.NewClient()
		defer s.Close()
		o := opi.NewOpi(s)
		o.Archive(os.Args[2], os.Args[3])
	}

}
