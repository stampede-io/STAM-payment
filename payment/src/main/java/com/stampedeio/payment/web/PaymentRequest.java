package com.stampedeio.payment.web;

import java.util.UUID;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record PaymentRequest(
        @NotNull UUID correlationId,
        @NotNull UUID aggregateId,
        @Min(1) long amountCents,
        @NotBlank @Pattern(regexp = "^[A-Za-z]{3}$") String currency,
        @NotBlank String paymentMethodId
) {
}
