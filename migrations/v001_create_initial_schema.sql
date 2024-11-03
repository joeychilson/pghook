-- V001_create_initial_schema.sql

CREATE SCHEMA IF NOT EXISTS pghook;

CREATE TABLE IF NOT EXISTS pghook.events (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION pghook.notify() RETURNS TRIGGER AS $$
DECLARE
    payload jsonb;
    id integer;
BEGIN
    payload := jsonb_build_object(
        'op', TG_OP,
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

    INSERT INTO pghook.events(payload) VALUES(payload) RETURNING pghook.events.id INTO id;
    payload := payload || jsonb_build_object('id', id);
    PERFORM pg_notify('pghook', payload::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;