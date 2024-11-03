package pghooks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Op string

const (
	InsertOp Op = "INSERT"
	UpdateOp Op = "UPDATE"
	DeleteOp Op = "DELETE"
)

type Hook struct {
	pool     *pgxpool.Pool
	handlers map[TableOp][]Handler
	mu       sync.Mutex
}

type TableOp struct {
	Table string `json:"table"`
	Op    Op     `json:"op"`
}

type Payload struct {
	Op     Op             `json:"op"`
	Schema string         `json:"schema"`
	Table  string         `json:"table"`
	Row    map[string]any `json:"row"`
	OldRow map[string]any `json:"old_row"`
}

// New creates a new Hook with a connection to the database using the given DSN.
func New(pool *pgxpool.Pool) (*Hook, error) {
	return &Hook{pool: pool, handlers: make(map[TableOp][]Handler)}, nil
}

// Hook adds a new handler for the given table and operation.
func (h *Hook) Hook(table string, op Op, handler Handler) {
	tableOp := TableOp{Table: table, Op: op}

	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.handlers[tableOp]; !ok {
		h.handlers[tableOp] = make([]Handler, 0)
	}
	h.handlers[tableOp] = append(h.handlers[tableOp], handler)
}

// InsertHook adds a new handler for the INSERT operation on the given table.
func (h *Hook) InsertHook(table string, handler HandlerFunc) {
	h.Hook(table, InsertOp, handler)
}

// UpdateHook adds a new handler for the UPDATE operation on the given table.
func (h *Hook) UpdateHook(table string, handler HandlerFunc) {
	h.Hook(table, UpdateOp, handler)
}

// DeleteHook adds a new handler for the DELETE operation on the given table.
func (h *Hook) DeleteHook(table string, handler HandlerFunc) {
	h.Hook(table, DeleteOp, handler)
}

// Listen starts listening for hook notifications and triggers the registered handlers for each operation.
func (h *Hook) Listen(ctx context.Context) error {
	if len(h.handlers) == 0 {
		return errors.New("no handlers registered")
	}

	if err := h.CreateFunction(ctx); err != nil {
		return fmt.Errorf("failed to create function: %w", err)
	}

	for tableOp := range h.handlers {
		if err := h.CreateTrigger(ctx, tableOp.Table, tableOp.Op); err != nil {
			return fmt.Errorf("failed to create trigger: %w", err)
		}
	}

	if _, err := h.pool.Exec(ctx, "LISTEN hooks"); err != nil {
		return fmt.Errorf("failed to listen to hooks: %w", err)
	}

	log.Println("Listening for notifications...")

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}

		go func() {
			payload := Payload{}
			if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
				log.Printf("failed to unmarshal payload: %v", err)
				return
			}

			op := TableOp{Table: payload.Table, Op: payload.Op}

			h.mu.Lock()
			handlers, ok := h.handlers[op]
			h.mu.Unlock()

			if !ok {
				log.Printf("no handlers registered for table operation %v", op)
				return
			}

			for _, h := range handlers {
				h.Handle(ctx, payload)
			}
		}()
	}
}

// Handler is an interface that handles a payload.
type Handler interface {
	Handle(ctx context.Context, payload Payload)
}

// HandlerFunc is a function that handles a payload. It implements the Handler interface.
type HandlerFunc func(ctx context.Context, payload Payload)

// Handle is a function that handles a payload.
func (hf HandlerFunc) Handle(ctx context.Context, payload Payload) {
	hf(ctx, payload)
}

func (h *Hook) CreateFunction(ctx context.Context) error {
	query := `
		CREATE OR REPLACE FUNCTION notify_hooks() RETURNS TRIGGER AS $$
		BEGIN
			PERFORM pg_notify(
				'hooks', 
				json_build_object(
					'op', TG_OP, 
					'schema', TG_TABLE_SCHEMA, 
					'table', TG_TABLE_NAME, 
					'row', row_to_json(NEW),
					'old_row', row_to_json(OLD)
				)::text
			);
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
	`
	if _, err := h.pool.Exec(ctx, query); err != nil {
		return err
	}

	log.Println("Created function notify_hooks")
	return nil
}

func (h *Hook) CreateTrigger(ctx context.Context, table string, op Op) error {
	query := `
		CREATE OR REPLACE TRIGGER ` + table + `_` + string(op) + `_hooks
		AFTER ` + string(op) + ` ON ` + table + `
		FOR EACH ROW
		EXECUTE PROCEDURE notify_hooks();
	`
	if _, err := h.pool.Exec(ctx, query); err != nil {
		return err
	}

	log.Printf("Created trigger %s_%s_hooks", table, op)
	return nil
}
