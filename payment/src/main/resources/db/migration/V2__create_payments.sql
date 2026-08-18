-- AC6: only psp_ref, status, amount_cents (+ currency) — never PAN, CVV, or card data.
-- Cardholder data lives in Stripe; payment_db stores only the reference.
CREATE TABLE payments (
    correlation_id  UUID         PRIMARY KEY,
    psp_ref         VARCHAR(120),
    status          VARCHAR(32)  NOT NULL,
    amount_cents    BIGINT       NOT NULL,
    currency        VARCHAR(8)   NOT NULL,
    failure_reason  VARCHAR(200),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX ix_payments_psp_ref ON payments (psp_ref);
