package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime/pprof"
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

	profile := os.Getenv("PROFILE")
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if os.Args[1] == "archive" {
		s := opi.NewClient()
		//s := opi.NewDB()
		defer s.Close()
		c := opi.NewSimpleCodec()
		o := opi.NewOpi(s, c)
		err := o.Archive(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Println(err)
		}
	}

	if os.Args[1] == "restore" {
		s := opi.NewClient()
		//s := opi.NewDB()
		defer s.Close()
		c := opi.NewSimpleCodec()
		o := opi.NewOpi(s, c)
		err := o.Restore(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Println(err)
		}
	}

}
