package com.stampedeio.payment.web;

import java.util.UUID;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

public record RefundHttpRequest(
        @NotNull UUID correlationId,
        @NotNull UUID aggregateId,
        UUID originalCorrelationId,
        String pspRef,
        @PositiveOrZero long amountCents
) {
}
