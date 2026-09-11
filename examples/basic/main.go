// Basic example: service lifecycle, cron scheduling, and graceful shutdown.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/lorenzo-vecchio/gorch"
)

type Worker struct {
	name string
}

func (w *Worker) Start(ctx gorch.ServiceContext) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			fmt.Printf("[%s] doing work…\n", w.name)
		}
	}
}

func (w *Worker) Stop() error {
	fmt.Printf("[%s] cleaning up\n", w.name)
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "gorch: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	orch := gorch.New()

	// Register a long-running service.
	must(orch.Register(&Worker{name: "worker-1"}))

	// Register a cron service that runs every 3 seconds, skipping overlapping ticks.
	must(orch.Register(&Worker{name: "cron-worker"},
		gorch.WithCron("@every 3s", gorch.CronSkip),
	))

	if err := orch.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		os.Exit(1)
	}

	// Wait for Ctrl+C.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	if err := orch.Stop(10 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "stop: %v\n", err)
	}
}
