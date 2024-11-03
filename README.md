# pghook

A Go library for listening to PostgreSQL notifications and executing hooks on insert, update, and delete events.

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
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joeychilson/pghook"
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	type User struct {
		ID        int       `json:"id"`
		Name      string    `json:"name"`
		Email     string    `json:"email"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
	}

	hook := pghook.New[User](pool)
	if err != nil {
		log.Fatal(err)
	}

	hook.OnInsert("users", func(ctx context.Context, e pghook.InsertEvent[User]) error {
		fmt.Println("inserted", e.Row)
		return nil
	})

	hook.OnUpdate("users", func(ctx context.Context, e pghook.UpdateEvent[User]) error {
		fmt.Println("updated", e.OldRow, e.NewRow)
		return nil
	})

	hook.OnDelete("users", func(ctx context.Context, e pghook.DeleteEvent[User]) error {
		fmt.Println("deleted", e.Row)
		return nil
	})

	if err := hook.Listen(ctx); err != nil {
		log.Fatal(err)
	}
}
```
