-- Schema bootstrap, executed by the postgres image on first start.
-- The API gateway and the consumer run the same (idempotent) DDL on start-up,
-- so the system also works against an empty database without this script.

CREATE TABLE IF NOT EXISTS inference_requests (
    request_id    TEXT PRIMARY KEY,
    model_id      TEXT NOT NULL DEFAULT 'default_model',
    input_data    TEXT NOT NULL,
    output_data   TEXT,
    status        TEXT NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    error_message TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_inference_requests_status
    ON inference_requests (status);
