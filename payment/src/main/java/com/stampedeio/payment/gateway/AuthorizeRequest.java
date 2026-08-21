package com.stampedeio.payment.gateway;

import java.util.UUID;

public record AuthorizeRequest(
        UUID correlationId,
        long amountCents,
        String currency,
        String paymentMethodId,
        String idempotencyKey
) {
}
