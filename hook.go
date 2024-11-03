package pghook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type eventHandler[T any] struct {
	table     string
	operation string
	handler   func(context.Context, any) error
}

// InsertEvent represents an INSERT operation event
type InsertEvent[T any] struct {
	Operation string `json:"operation"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Row       T      `json:"row"`
}

// UpdateEvent represents an UPDATE operation event
type UpdateEvent[T any] struct {
	Operation string `json:"operation"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	OldRow    T      `json:"old_row"`
	NewRow    T      `json:"new_row"`
}

// DeleteEvent represents a DELETE operation event
type DeleteEvent[T any] struct {
	Operation string `json:"operation"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Row       T      `json:"row"`
}

// Option is a function that configures a Hook
type Option func(*Hook[any])

// WithLogger sets the logger for the Hook
func WithLogger(logger *slog.Logger) Option {
	return func(h *Hook[any]) { h.logger = logger }
}

// WithChannel sets the notification channel name
func WithChannel(channel string) Option {
	return func(h *Hook[any]) { h.channel = channel }
}

// Hook manages PostgreSQL triggers and notifications for table changes
type Hook[T any] struct {
	pool     *pgxpool.Pool
	logger   *slog.Logger
	channel  string
	mu       sync.RWMutex
	handlers []eventHandler[T]
}

// New creates a new Hook with the given configuration
func New[T any](pool *pgxpool.Pool, opts ...Option) *Hook[T] {
	tempHook := &Hook[any]{
		pool:    pool,
		logger:  slog.Default(),
		channel: "pghook",
	}
	for _, opt := range opts {
		opt(tempHook)
	}
	return &Hook[T]{pool: tempHook.pool, logger: tempHook.logger, channel: tempHook.channel}
}

// OnInsert adds a new handler for the INSERT operation on the given table.
func (h *Hook[T]) OnInsert(table string, handler func(context.Context, InsertEvent[T]) error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers = append(h.handlers, eventHandler[T]{
		table:     table,
		operation: "INSERT",
		handler:   func(ctx context.Context, e any) error { return handler(ctx, e.(InsertEvent[T])) },
	})
}

// OnUpdate adds a new handler for the UPDATE operation on the given table.
func (h *Hook[T]) OnUpdate(table string, handler func(context.Context, UpdateEvent[T]) error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers = append(h.handlers, eventHandler[T]{
		table:     table,
		operation: "UPDATE",
		handler:   func(ctx context.Context, e any) error { return handler(ctx, e.(UpdateEvent[T])) },
	})
}

// OnDelete adds a new handler for the DELETE operation on the given table.
func (h *Hook[T]) OnDelete(table string, handler func(context.Context, DeleteEvent[T]) error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers = append(h.handlers, eventHandler[T]{
		table:     table,
		operation: "DELETE",
		handler:   func(ctx context.Context, e any) error { return handler(ctx, e.(DeleteEvent[T])) },
	})
}

// Listen starts listening for notifications and handles them with registered handlers
func (h *Hook[T]) Listen(ctx context.Context) error {
	if err := h.setup(ctx); err != nil {
		return fmt.Errorf("failed to setup hook: %w", err)
	}

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	h.logger.Info("listening for notifications...", "channel", h.channel)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				return fmt.Errorf("wait for notification failed: %w", err)
			}
			go h.handleNotification(ctx, notification.Payload)
		}
	}
}

func (h *Hook[T]) setup(ctx context.Context) error {
	if err := h.createNotifyFunction(ctx); err != nil {
		return fmt.Errorf("create notify function failed: %w", err)
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

	if _, err := h.pool.Exec(ctx, fmt.Sprintf("LISTEN %s", h.channel)); err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	h.logger.Debug("setup completed", "channel", h.channel, "tables", len(tables))
	return nil
}

func (h *Hook[T]) handleNotification(ctx context.Context, payloadStr string) {
	var base struct {
		Operation string `json:"operation"`
		Table     string `json:"table"`
	}
	if err := json.Unmarshal([]byte(payloadStr), &base); err != nil {
		h.logger.Error("unmarshal notification payload failed", "error", err)
		return
	}

	var event any
	switch base.Operation {
	case "INSERT":
		var e InsertEvent[T]
		if err := json.Unmarshal([]byte(payloadStr), &e); err != nil {
			h.logger.Error("unmarshal insert payload failed", "error", err)
			return
		}
		event = e
	case "UPDATE":
		var e UpdateEvent[T]
		if err := json.Unmarshal([]byte(payloadStr), &e); err != nil {
			h.logger.Error("unmarshal update payload failed", "error", err)
			return
		}
		event = e
	case "DELETE":
		var e DeleteEvent[T]
		if err := json.Unmarshal([]byte(payloadStr), &e); err != nil {
			h.logger.Error("unmarshal delete payload failed", "error", err)
			return
		}
		event = e
	default:
		h.logger.Debug("unsupported operation", "operation", base.Operation)
		return
	}

	for _, handler := range h.handlers {
		if handler.table == base.Table && handler.operation == base.Operation {
			if err := handler.handler(ctx, event); err != nil {
				h.logger.Error("handler failed", "table", base.Table, "operation", base.Operation, "error", err)
			}
		}
	}
}

func (h *Hook[T]) createNotifyFunction(ctx context.Context) error {
	query := fmt.Sprintf(`
        CREATE OR REPLACE FUNCTION pghook_notify() RETURNS TRIGGER AS $$
        DECLARE
            payload jsonb;
        BEGIN
            payload := jsonb_build_object(
                'operation', TG_OP,
                'schema', TG_TABLE_SCHEMA,
                'table', TG_TABLE_NAME
            );
            CASE TG_OP
                WHEN 'INSERT' THEN
                    payload := payload || jsonb_build_object('row', row_to_json(NEW));
                WHEN 'UPDATE' THEN
                    payload := payload || jsonb_build_object(
                        'old_row', row_to_json(OLD),
                        'new_row', row_to_json(NEW)
                    );
                WHEN 'DELETE' THEN
                    payload := payload || jsonb_build_object('row', row_to_json(OLD));
            END CASE;
            PERFORM pg_notify('%s', payload::text);
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    `, h.channel)

	if _, err := h.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create notify function failed: %w", err)
	}
	h.logger.Debug("created notify function", "name", "pghook_notify", "channel", h.channel)
	return nil
}

func (h *Hook[T]) createTrigger(ctx context.Context, table string) error {
	query := fmt.Sprintf(`
        CREATE OR REPLACE TRIGGER %s_pghook
        AFTER INSERT OR UPDATE OR DELETE ON %s
        FOR EACH ROW EXECUTE PROCEDURE pghook_notify();
    `, table, table)

	if _, err := h.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create trigger failed: %w", err)
	}

	h.logger.Debug("created trigger", "name", fmt.Sprintf("%s_pghook", table), "table", table)
	return nil
}
