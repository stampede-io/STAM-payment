CREATE TABLE idempotency_keys (
    correlation_id  UUID        PRIMARY KEY,
    command_type    VARCHAR(80) NOT NULL,
    outcome         VARCHAR(40) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
