-- Store the caller-supplied aggregate id (reservationId for the booking saga path)
-- so the webhook-driven outcome event can be keyed on the same aggregate as the
-- original authorize event. Nullable because pre-existing rows won't have it.
ALTER TABLE payments ADD COLUMN aggregate_id UUID;

-- Cache the psp_ref associated with an idempotent outcome so a duplicate command
-- can replay a byte-parity payload (refundRef, pspRef) without re-hitting the PSP.
ALTER TABLE idempotency_keys ADD COLUMN psp_ref VARCHAR(120);
