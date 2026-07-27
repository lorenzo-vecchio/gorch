// Pub-sub example: services communicating via topics through the Messenger.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/lorenzo-vecchio/gorch/gorch"
)

// Publisher emits events on the "events" topic every second.
type Publisher struct{}

func (p *Publisher) Start(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// ctx is a ServiceContext (it embeds context.Context).
			sc := ctx.(gorch.ServiceContext)
			sc.Messenger.Publish(fmt.Sprintf("event #%d", i), "events")
			sc.Logger.Info("published event", "seq", i)
		}
	}
}

func (p *Publisher) Stop() error { return nil }

// Subscriber listens on the "events" topic.
type Subscriber struct {
	name string
}

func (s *Subscriber) Start(ctx context.Context) error {
	sc := ctx.(gorch.ServiceContext)
	ch, unsub := sc.Messenger.Subscribe("events")
	defer unsub()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-ch:
			sc.Logger.Info("received", "msg", msg, "subscriber", s.name)
		}
	}
}

func (s *Subscriber) Stop() error { return nil }

func main() {
	orch := gorch.New(gorch.Config{LogLevel: gorch.LogLevelDebug})

	orch.Register(&Publisher{})
	orch.Register(&Subscriber{name: "sub-1"})
	orch.Register(&Subscriber{name: "sub-2"})

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
