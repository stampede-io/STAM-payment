package com.stampedeio.payment.web;

import com.stampedeio.payment.gateway.ChargeStatus;

public record PaymentResponse(
        String pspRef,
        ChargeStatus status,
        String failureReason
) {
}
