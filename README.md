# STAM-payment

[![CI](https://github.com/stampede-io/STAM-payment/actions/workflows/ci.yml/badge.svg)](https://github.com/stampede-io/STAM-payment/actions/workflows/ci.yml)

Payment service for STAMPEDE — authorizes and refunds against Stripe in test
mode by default, with a MockPaymentGateway alternative for chaos and load
testing. Consumes `payments.commands` from the saga orchestrator and emits
`PaymentAuthorized` / `PaymentFailed` / `RefundIssued` / `RefundFailed`
on `payments.events`.

## Endpoints

| Method | Path                            | Purpose                                                  |
|--------|---------------------------------|----------------------------------------------------------|
| POST   | `/api/v1/payments`              | Charge a PaymentMethod, emits `PaymentAuthorized/Failed` |
| POST   | `/api/v1/payments/refunds`      | Refund a prior charge                                    |
| POST   | `/api/v1/payments/webhook`      | Stripe webhook receiver (signature-verified)             |

## Configuration

| Property                | Env                    | Notes                                        |
|-------------------------|------------------------|----------------------------------------------|
| `payment.gateway`       | `PAYMENT_GATEWAY`      | `mock` (default) or `stripe`                 |
| `payment.failure-rate`  | `PAYMENT_FAILURE_RATE` | Mock gateway: probability in `[0.0, 1.0]`    |
| `stripe.api-key`        | `STRIPE_API_KEY`       | Must be a `sk_test_*` / `rk_test_*` key      |
| `stripe.webhook-secret` | `STRIPE_WEBHOOK_SECRET`| The `whsec_*` shared with the Stripe endpoint|

## Security

**No cardholder data is ever stored in `payment_db`.** The `payments` table
persists only:

  - `psp_ref` — the Stripe `PaymentIntent` id (`pi_*`)
  - `status` — `PENDING` / `AUTHORIZED` / `REQUIRES_ACTION` / `FAILED` / `REFUNDED`
  - `amount_cents`, `currency`
  - `failure_reason` — Stripe decline code or gateway error code (never PAN)
  - `created_at` / `updated_at`

There is no column for card numbers, CVV/CVC, expiry, cardholder name, or
any other PAN element. The Flyway migration `V2__create_payments.sql`
defines the table; adding any of the above fields would be a PCI-scope
change and requires DPO sign-off.

Cardholder data enters the flow only as a Stripe `PaymentMethod` id
(`pm_*`) generated client-side by Stripe.js — it never touches our
request body, service memory beyond the outgoing Stripe call, or logs.
The `StripePaymentGateway` refuses to boot with a live-mode key
(`sk_live_*`) as a defence-in-depth guardrail while the service is not
yet PCI-scoped.

**Webhook signatures are always verified** with HMAC-SHA256 against
`stripe.webhook-secret` before any payload is trusted. Requests with
missing, malformed, or mismatched signatures return HTTP 400 and are
never persisted or processed. The raw request body is consumed verbatim
so that no serializer round-trip can invalidate the signature.

## Local development

Java services keep their Maven project in `payment/`, not at repo root:

```bash
cd payment
./mvnw -B verify
```

The IT suite (`StripePaymentFlowIT`, `PaymentCommandConsumerIT`) uses
Testcontainers and needs Docker running.

## Running against real Stripe test mode

Export the test key (get one from the Stripe dashboard while it's toggled
to test mode) before starting the service:

```bash
export PAYMENT_GATEWAY=stripe
export STRIPE_API_KEY=sk_test_xxx
export STRIPE_WEBHOOK_SECRET=whsec_xxx
```

For the CI job to hit real Stripe, the repo needs a `STRIPE_TEST_KEY`
GitHub Actions secret. Without it, CI runs the IT against the mock
gateway (same wiring, same assertions) so the pipeline still stays green.
