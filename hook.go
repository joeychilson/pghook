package pghook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/joeychilson/pgmq"
)

const (
	defaultQueueName         = "pghook_events"
	defaultVisibilityTimeout = 30 * time.Second
	defaultPollInterval      = 5 * time.Second
	defaultPollTimeout       = 250 * time.Millisecond
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
	querier  pgmq.Querier
	logger   *slog.Logger
	handlers []eventHandler[T]
	mu       sync.RWMutex
}

// New creates a new Hook with the given configuration
func New[T any](querier pgmq.Querier, opts ...Option) *Hook[T] {
	tempHook := &Hook[any]{querier: querier, logger: slog.Default()}
	for _, opt := range opts {
		opt(tempHook)
	}
	return &Hook[T]{querier: tempHook.querier, logger: tempHook.logger}
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
	queue, err := pgmq.New[json.RawMessage](ctx, h.querier, defaultQueueName)
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	if err := queue.Create(ctx); err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	if err := h.createTriggerFunction(ctx); err != nil {
		return fmt.Errorf("failed to create trigger function: %w", err)
	}

	for _, handler := range h.handlers {
		if err := h.createTrigger(ctx, handler.table); err != nil {
			return fmt.Errorf("failed to create trigger: %w", err)
		}
	}

	h.logger.Info("listening for events...")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := queue.ReadWithPoll(ctx, defaultVisibilityTimeout, defaultPollInterval, defaultPollTimeout)
			if err != nil {
				h.logger.Error("failed to read message", "error", err)
				continue
			}
			if msg == nil {
				h.logger.Debug("no messages in queue")
				continue
			}

			if err := h.handleEvent(ctx, msg.Message); err != nil {
				h.logger.Error("failed to handle event", "error", err)
				continue
			}

			if err := queue.Delete(ctx, msg.ID); err != nil {
				h.logger.Error("failed to delete message", "error", err)
			}
		}
	}
}

func (h *Hook[T]) handleEvent(ctx context.Context, rawEvent json.RawMessage) error {
	var base baseEvent
	if err := json.Unmarshal(rawEvent, &base); err != nil {
		return fmt.Errorf("unmarshal base event failed: %w", err)
	}

	var event any
	switch base.Op {
	case "INSERT":
		var e InsertEvent[T]
		if err := json.Unmarshal(rawEvent, &e); err != nil {
			return fmt.Errorf("unmarshal insert event failed: %w", err)
		}
		event = e
	case "UPDATE":
		var e UpdateEvent[T]
		if err := json.Unmarshal(rawEvent, &e); err != nil {
			return fmt.Errorf("unmarshal update event failed: %w", err)
		}
		event = e
	case "DELETE":
		var e DeleteEvent[T]
		if err := json.Unmarshal(rawEvent, &e); err != nil {
			return fmt.Errorf("unmarshal delete event failed: %w", err)
		}
		event = e
	default:
		return fmt.Errorf("unsupported operation: %s", base.Op)
	}

	for _, handler := range h.handlers {
		if handler.table == base.Table && handler.op == base.Op {
			if err := handler.handler(ctx, event); err != nil {
				return fmt.Errorf("handler failed: %w", err)
			}
		}
	}
	return nil
}

func (h *Hook[T]) createTriggerFunction(ctx context.Context) error {
	query := `
		CREATE OR REPLACE FUNCTION pghook.notify()
		RETURNS trigger AS $$
		DECLARE
			payload jsonb;
		BEGIN
			IF TG_OP = 'INSERT' THEN
				payload = jsonb_build_object(
					'op', TG_OP,
					'table', TG_TABLE_NAME,
					'row', row_to_json(NEW)
				);
			ELSIF TG_OP = 'UPDATE' THEN
				payload = jsonb_build_object(
					'op', TG_OP,
					'table', TG_TABLE_NAME,
					'old_row', row_to_json(OLD),
					'new_row', row_to_json(NEW)
				);
			ELSIF TG_OP = 'DELETE' THEN
				payload = jsonb_build_object(
					'op', TG_OP,
					'table', TG_TABLE_NAME,
					'row', row_to_json(OLD)
				);
			END IF;
			
			PERFORM pgmq.send('pghook_events', payload);
			
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
	`
	if _, err := h.querier.Exec(ctx, query); err != nil {
		return fmt.Errorf("create trigger function failed: %w", err)
	}
	h.logger.Debug("created trigger function")
	return nil
}

func (h *Hook[T]) createTrigger(ctx context.Context, table string) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TRIGGER pghook_%s AFTER INSERT OR UPDATE OR DELETE ON %s
		FOR EACH ROW EXECUTE PROCEDURE pghook.notify();
    `, table, table)
	if _, err := h.querier.Exec(ctx, query); err != nil {
		return fmt.Errorf("create trigger failed: %w", err)
	}
	h.logger.Debug("created trigger", "name", fmt.Sprintf("pghook_%s", table), "table", table)
	return nil
}
