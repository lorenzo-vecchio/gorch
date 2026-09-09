// Typed request-reply example: a service handles typed requests via
// TypedSubscribeRequest + TypedRespond, and a requester issues a TypedRequest.
package main

import (
	"fmt"
	"time"

	"github.com/lorenzo-vecchio/gorch/gorch"
)

type CreateOrderReq struct {
	ItemID string
	Qty    int
}

type CreateOrderResp struct {
	OrderID string
	Status  string
}

// OrderService handles create-order requests on the "orders.create" topic.
type OrderService struct{}

func (s *OrderService) Start(ctx gorch.ServiceContext) error {
	reqCh, unsub := gorch.TypedSubscribeRequest[CreateOrderReq](ctx.Messenger, "orders.create")
	defer unsub()
	for env := range reqCh {
		ctx.Logger.Info("handling order", "item", env.Value.ItemID, "qty", env.Value.Qty)
		gorch.TypedRespond(ctx.Messenger, CreateOrderResp{OrderID: "ord-123", Status: "created"}, env.ReplyTopic)
	}
	return nil
}

func (s *OrderService) Stop() error { return nil }

// Requester issues a single typed request, then idles until shutdown.
type Requester struct{}

func (r *Requester) Start(ctx gorch.ServiceContext) error {
	// Give the responder a moment to subscribe before requesting.
	time.Sleep(50 * time.Millisecond)

	resp, err := gorch.TypedRequest[CreateOrderReq, CreateOrderResp](
		ctx.Messenger, ctx, CreateOrderReq{ItemID: "abc", Qty: 2}, "orders.create",
	)
	if err != nil {
		return err
	}
	ctx.Logger.Info("order created", "orderID", resp.OrderID, "status", resp.Status)
	<-ctx.Done()
	return nil
}

func (r *Requester) Stop() error { return nil }

func main() {
	orch := gorch.New(gorch.WithLogLevel(gorch.LogLevelInfo))
	if err := orch.Register(&OrderService{}, gorch.WithName("orders")); err != nil {
		panic(err)
	}
	if err := orch.Register(&Requester{}, gorch.WithName("requester"), gorch.DependsOn("orders")); err != nil {
		panic(err)
	}

	if err := orch.Start(); err != nil {
		panic(err)
	}

	// Let the request-reply complete, then shut down.
	time.Sleep(500 * time.Millisecond)
	if err := orch.Stop(5 * time.Second); err != nil {
		fmt.Printf("stop error: %v\n", err)
	}
}
