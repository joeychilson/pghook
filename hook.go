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
	defaultEventQueueName    = "pghook_events"
	defaultVisibilityTimeout = 30 * time.Second
	defaultPollInterval      = 5 * time.Second
	defaultPollTimeout       = 250 * time.Millisecond
)

type op string

const (
	opInsert op = "INSERT"
	opUpdate op = "UPDATE"
	opDelete op = "DELETE"
)

type eventHandler[T any] struct {
	op      op
	table   string
	handler func(context.Context, any) error
}

type baseEvent struct {
	ID    int    `json:"id"`
	Op    op     `json:"op"`
	Table string `json:"table"`
}

// InsertPayload represents a database insert operation event with the new row data
type InsertPayload[T any] struct {
	baseEvent
	Row *T `json:"row"`
}

// DeletePayload represents a database delete operation event with the deleted row data
type UpdatePayload[T any] struct {
	baseEvent
	OldRow *T `json:"old_row"`
	NewRow *T `json:"new_row"`
}

// DeletePayload represents a DELETE operation payload
type DeletePayload[T any] struct {
	baseEvent
	Row *T `json:"row"`
}

// Option defines a function type for configuring a Hook instance
type Option func(*Hook[any])

// WithLogger sets a custom logger for the Hook instance
func WithLogger(logger *slog.Logger) Option {
	return func(h *Hook[any]) { h.logger = logger }
}

// Hook manages PostgreSQL triggers and message queue-based notifications for table changes
type Hook[T any] struct {
	querier       pgmq.Querier
	logger        *slog.Logger
	eventHandlers []eventHandler[T]
	mutex         sync.RWMutex
}

// New creates a new Hook instance with the provided database querier and options
func New[T any](querier pgmq.Querier, opts ...Option) *Hook[T] {
	hookConfig := &Hook[any]{querier: querier, logger: slog.Default()}
	for _, opt := range opts {
		opt(hookConfig)
	}
	return &Hook[T]{querier: hookConfig.querier, logger: hookConfig.logger}
}

// OnInsert registers a handler function for INSERT operations on the specified table
func (h *Hook[T]) OnInsert(table string, handler func(context.Context, InsertPayload[T]) error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.eventHandlers = append(h.eventHandlers, eventHandler[T]{
		op:      opInsert,
		table:   table,
		handler: func(ctx context.Context, p any) error { return handler(ctx, p.(InsertPayload[T])) },
	})
}

// OnUpdate registers a handler function for UPDATE operations on the specified table
func (h *Hook[T]) OnUpdate(table string, handler func(context.Context, UpdatePayload[T]) error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.eventHandlers = append(h.eventHandlers, eventHandler[T]{
		op:      opUpdate,
		table:   table,
		handler: func(ctx context.Context, p any) error { return handler(ctx, p.(UpdatePayload[T])) },
	})
}

// OnDelete registers a handler function for DELETE operations on the specified table
func (h *Hook[T]) OnDelete(table string, handler func(context.Context, DeletePayload[T]) error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.eventHandlers = append(h.eventHandlers, eventHandler[T]{
		op:      opDelete,
		table:   table,
		handler: func(ctx context.Context, p any) error { return handler(ctx, p.(DeletePayload[T])) },
	})
}

// Listen begins processing database events and dispatching them to registered handlers.
func (h *Hook[T]) Listen(ctx context.Context) error {
	queue, err := pgmq.New[json.RawMessage](ctx, h.querier, defaultEventQueueName)
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	if err := queue.Create(ctx); err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	if err := h.createNotificationFunction(ctx); err != nil {
		return fmt.Errorf("failed to create trigger function: %w", err)
	}

	for _, handler := range h.eventHandlers {
		if err := h.createTableTrigger(ctx, handler.table); err != nil {
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

			if err := h.processPayload(ctx, msg.Message); err != nil {
				h.logger.Error("failed to process payload", "error", err)
				continue
			}

			if err := queue.Delete(ctx, msg.ID); err != nil {
				h.logger.Error("failed to delete message", "error", err)
			}
		}
	}
}

func (h *Hook[T]) processPayload(ctx context.Context, rawPayload json.RawMessage) error {
	var base baseEvent
	if err := json.Unmarshal(rawPayload, &base); err != nil {
		return fmt.Errorf("unmarshal base payload failed: %w", err)
	}

	var payload any
	switch base.Op {
	case opInsert:
		var p InsertPayload[T]
		if err := json.Unmarshal(rawPayload, &p); err != nil {
			return fmt.Errorf("unmarshal insert payload failed: %w", err)
		}
		payload = p
	case opUpdate:
		var p UpdatePayload[T]
		if err := json.Unmarshal(rawPayload, &p); err != nil {
			return fmt.Errorf("unmarshal update payload failed: %w", err)
		}
		payload = p
	case opDelete:
		var p DeletePayload[T]
		if err := json.Unmarshal(rawPayload, &p); err != nil {
			return fmt.Errorf("unmarshal delete payload failed: %w", err)
		}
		payload = p
	default:
		return fmt.Errorf("unsupported operation: %s", base.Op)
	}

	for _, handler := range h.eventHandlers {
		if handler.table == base.Table && handler.op == base.Op {
			if err := handler.handler(ctx, payload); err != nil {
				return fmt.Errorf("handler failed: %w", err)
			}
		}
	}
	return nil
}

func (h *Hook[T]) createNotificationFunction(ctx context.Context) error {
	query := fmt.Sprintf(`
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
			
			PERFORM pgmq.send('%s', payload);
			
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
	`, defaultEventQueueName)
	if _, err := h.querier.Exec(ctx, query); err != nil {
		return fmt.Errorf("create trigger function failed: %w", err)
	}
	h.logger.Debug("created trigger function pghook.notify")
	return nil
}

func (h *Hook[T]) createTableTrigger(ctx context.Context, table string) error {
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
