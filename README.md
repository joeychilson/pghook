# pghook

A Go library for executing hooks on insert, update, and delete events in PostgreSQL.

## Installation

```bash
go get github.com/joeychilson/pghook
```

## Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joeychilson/pghook"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	type User struct {
		ID        int    `json:"id"`
		Name      string `json:"name"`
		CreatedAt string `json:"created_at"`
	}

	hook := pghook.New[User](pool, pghook.WithLogger(logger))
	if err != nil {
		log.Fatal(err)
	}

	hook.OnInsert("users", func(ctx context.Context, e pghook.InsertPayload[User]) error {
		fmt.Println("inserted", e.Row)
		return nil
	})

	hook.OnUpdate("users", func(ctx context.Context, e pghook.UpdatePayload[User]) error {
		fmt.Println("updated", e.OldRow, e.NewRow)
		return nil
	})

	hook.OnDelete("users", func(ctx context.Context, e pghook.DeletePayload[User]) error {
		fmt.Println("deleted", e.Row)
		return nil
	})

	if err := hook.Listen(ctx); err != nil {
		log.Fatal(err)
	}
}
```
