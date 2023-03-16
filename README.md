# pghooks

A simple PostgreSQL LISTEN/NOTIFY library for Go.

## Installation

```bash
go get github.com/joeychilson/pghooks
```

## Example

```go
package main

import (
	"context"
	"log"

	"github.com/joeychilson/pghooks"
)

type MyHook struct{}

func (h *MyHook) Handle(ctx context.Context, payload pghooks.Payload) {
	log.Println("MyHook received notification on table", payload.Table)
	log.Printf("Payload: %+v", payload)
}

func main() {
	ctx := context.Background()

	// Create a new hooks instance. Use NewWithPool to use an existing connection pool.
	h, err := pghooks.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}

	// Insert hook for the "users" table.
	h.InsertHook("users", func(ctx context.Context, payload pghooks.Payload) {
		log.Println("HandleInsert received INSERT notification on table", payload.Table)
		log.Printf("Payload: %+v", payload)
	})

	// Update hook for the "users" table.
	h.UpdateHook("users", func(ctx context.Context, payload pghooks.Payload) {
		log.Println("HandleUpdate received UPDATE notification on table", payload.Table)
		log.Printf("Payload: %+v", payload)
	})

	// Delete hook for the "users" table.
	h.DeleteHook("users", func(ctx context.Context, payload pghooks.Payload) {
		log.Println("HandleDelete received DELETE notification on table", payload.Table)
		log.Printf("Payload: %+v", payload)
	})

	// Register the custom handler to handle INSERT operations on the "users" table.
	h.Hook("users", pghooks.InsertOp, &MyHook{})

	// Start listening for notifications
	if err := h.Listen(ctx); err != nil {
		log.Fatal(err)
	}
}
```
