package com.stampedeio.payment.gateway;

import java.util.UUID;

public record RefundRequest(
        UUID correlationId,
        String pspRef,
        long amountCents,
        String idempotencyKey
) {
}
