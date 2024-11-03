package pghook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joeychilson/pghook/migrations"
)

type eventHandler[T any] struct {
	op      string
	table   string
	handler func(context.Context, any) error
}

type baseEvent struct {
	ID    int    `json:"id"`
	Op    string `json:"op"`
	Table string `json:"table"`
}

// InsertEvent represents an INSERT operation event
type InsertEvent[T any] struct {
	baseEvent
	Row T `json:"row"`
}

// UpdateEvent represents an UPDATE operation event
type UpdateEvent[T any] struct {
	baseEvent
	OldRow T `json:"old_row"`
	NewRow T `json:"new_row"`
}

// DeleteEvent represents a DELETE operation event
type DeleteEvent[T any] struct {
	baseEvent
	Row T `json:"row"`
}

// Option is a function that configures a Hook
type Option func(*Hook[any])

// WithLogger sets the logger for the Hook
func WithLogger(logger *slog.Logger) Option {
	return func(h *Hook[any]) { h.logger = logger }
}

// Hook manages PostgreSQL triggers and notifications for table changes
type Hook[T any] struct {
	pool     *pgxpool.Pool
	logger   *slog.Logger
	migrator *migrations.Migrator
	handlers []eventHandler[T]
	mu       sync.RWMutex
}

// New creates a new Hook with the given configuration
func New[T any](pool *pgxpool.Pool, opts ...Option) *Hook[T] {
	tempHook := &Hook[any]{pool: pool, logger: slog.Default()}
	for _, opt := range opts {
		opt(tempHook)
	}
	return &Hook[T]{
		pool:     tempHook.pool,
		logger:   tempHook.logger,
		migrator: migrations.NewMigrator(tempHook.pool),
	}
}

// OnInsert adds a new handler for the INSERT operation on the given table.
func (h *Hook[T]) OnInsert(table string, handler func(context.Context, InsertEvent[T]) error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers = append(h.handlers, eventHandler[T]{
		op:      "INSERT",
		table:   table,
		handler: func(ctx context.Context, e any) error { return handler(ctx, e.(InsertEvent[T])) },
	})
}

// OnUpdate adds a new handler for the UPDATE operation on the given table.
func (h *Hook[T]) OnUpdate(table string, handler func(context.Context, UpdateEvent[T]) error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers = append(h.handlers, eventHandler[T]{
		op:      "UPDATE",
		table:   table,
		handler: func(ctx context.Context, e any) error { return handler(ctx, e.(UpdateEvent[T])) },
	})
}

// OnDelete adds a new handler for the DELETE operation on the given table.
func (h *Hook[T]) OnDelete(table string, handler func(context.Context, DeleteEvent[T]) error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers = append(h.handlers, eventHandler[T]{
		op:      "DELETE",
		table:   table,
		handler: func(ctx context.Context, e any) error { return handler(ctx, e.(DeleteEvent[T])) },
	})
}

// Listen starts listening for notifications and handles them with registered handlers
func (h *Hook[T]) Listen(ctx context.Context) error {
	if err := h.setup(ctx); err != nil {
		return fmt.Errorf("failed to setup hook: %w", err)
	}

	if err := h.processUnprocessedEvents(ctx); err != nil {
		return fmt.Errorf("failed to process unprocessed events: %w", err)
	}

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	h.logger.Info("listening for notifications...")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				return fmt.Errorf("wait for notification failed: %w", err)
			}
			go h.handleEvent(ctx, eventSource{Payload: notification.Payload, EventID: 0})
		}
	}
}

func (h *Hook[T]) setup(ctx context.Context) error {
	if err := h.migrator.Migrate(context.Background()); err != nil {
		return fmt.Errorf("failed to migrate: %w", err)
	}

	tables := make(map[string]bool)
	for _, handler := range h.handlers {
		tables[handler.table] = true
	}

	for table := range tables {
		if err := h.createTrigger(ctx, table); err != nil {
			return fmt.Errorf("create trigger for table %s failed: %w", table, err)
		}
	}

	if _, err := h.pool.Exec(ctx, "LISTEN pghook"); err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	h.logger.Debug("setup completed", "tables", len(tables))
	return nil
}

func (h *Hook[T]) processUnprocessedEvents(ctx context.Context) error {
	rows, err := h.pool.Query(ctx, "SELECT id, payload FROM pghook.events WHERE processed = false ORDER BY id")
	if err != nil {
		return fmt.Errorf("failed to query unprocessed events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id      int
			payload []byte
		)
		if err := rows.Scan(&id, &payload); err != nil {
			h.logger.Error("failed to scan event", "error", err)
			continue
		}

		if err := h.handleEvent(ctx, eventSource{Payload: string(payload), EventID: id}); err != nil {
			h.logger.Error("failed to handle unprocessed event", "error", err, "event_id", id)
		}
	}
	return rows.Err()
}

type eventSource struct {
	Payload string
	EventID int
}

func (h *Hook[T]) handleEvent(ctx context.Context, source eventSource) error {
	var base baseEvent
	if err := json.Unmarshal([]byte(source.Payload), &base); err != nil {
		h.logger.Error("unmarshal payload failed", "error", err)
		return err
	}

	var event any
	switch base.Op {
	case "INSERT":
		var e InsertEvent[T]
		if err := json.Unmarshal([]byte(source.Payload), &e); err != nil {
			h.logger.Error("unmarshal insert event failed", "error", err)
			return err
		}
		event = e
	case "UPDATE":
		var e UpdateEvent[T]
		if err := json.Unmarshal([]byte(source.Payload), &e); err != nil {
			h.logger.Error("unmarshal update event failed", "error", err)
			return err
		}
		event = e
	case "DELETE":
		var e DeleteEvent[T]
		if err := json.Unmarshal([]byte(source.Payload), &e); err != nil {
			h.logger.Error("unmarshal delete event failed", "error", err)
			return err
		}
		event = e
	default:
		h.logger.Debug("unsupported op", "op", base.Op)
		return nil
	}

	for _, handler := range h.handlers {
		if handler.table == base.Table && handler.op == base.Op {
			if err := handler.handler(ctx, event); err != nil {
				h.logger.Error("handler failed", "table", base.Table, "op", base.Op, "error", err)
			}
		}
	}

	if source.EventID > 0 {
		if _, err := h.pool.Exec(ctx, "UPDATE pghook.events SET processed = true WHERE id = $1", source.EventID); err != nil {
			h.logger.Error("failed to mark event as processed", "event_id", source.EventID, "error", err)
			return err
		}
	}
	return nil
}

func (h *Hook[T]) createTrigger(ctx context.Context, table string) error {
	query := fmt.Sprintf(`
        CREATE OR REPLACE TRIGGER pghook_%s AFTER INSERT OR UPDATE OR DELETE ON %s
        FOR EACH ROW EXECUTE PROCEDURE pghook.notify();
    `, table, table)

	if _, err := h.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create trigger failed: %w", err)
	}

	h.logger.Debug("created trigger", "name", fmt.Sprintf("pghook_%s", table), "table", table)
	return nil
}
