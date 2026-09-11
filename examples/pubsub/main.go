// Pub-sub example: services communicating via topics through the Messenger.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/lorenzo-vecchio/gorch"
)

// Publisher emits events on the "events" topic every second.
type Publisher struct{}

func (p *Publisher) Start(ctx gorch.ServiceContext) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ctx.Messenger.Publish(fmt.Sprintf("event #%d", i), "events")
			ctx.Logger.Info("published event", "seq", i)
		}
	}
}

func (p *Publisher) Stop() error { return nil }

// Subscriber listens on the "events" topic.
type Subscriber struct {
	name string
}

func (s *Subscriber) Start(ctx gorch.ServiceContext) error {
	ch, unsub := ctx.Messenger.Subscribe("events")
	defer unsub()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-ch:
			ctx.Logger.Info("received", "msg", msg, "subscriber", s.name)
		}
	}
}

func (s *Subscriber) Stop() error { return nil }

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "gorch: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	orch := gorch.New(gorch.WithLogLevel(gorch.LogLevelDebug))

	must(orch.Register(&Publisher{}))
	must(orch.Register(&Subscriber{name: "sub-1"}))
	must(orch.Register(&Subscriber{name: "sub-2"}))

	if err := orch.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	if err := orch.Stop(10 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "stop: %v\n", err)
	}
}
