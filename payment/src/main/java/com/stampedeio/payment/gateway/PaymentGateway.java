package com.stampedeio.payment.gateway;

/**
 * PSP-agnostic charge/refund contract. Two implementations coexist:
 * StripePaymentGateway (real Stripe test/prod mode) and MockPaymentGateway
 * (deterministic-random failure injection for chaos tests). Selection is via
 * the {@code payment.gateway} property.
 */
public interface PaymentGateway {

    AuthorizeResult authorize(AuthorizeRequest request);

    RefundResult refund(RefundRequest request);

    String name();
}
