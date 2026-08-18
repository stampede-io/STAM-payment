package com.stampedeio.payment.gateway;

public record AuthorizeResult(
        String pspRef,
        ChargeStatus status,
        String failureReason
) {
    public static AuthorizeResult authorized(String pspRef) {
        return new AuthorizeResult(pspRef, ChargeStatus.AUTHORIZED, null);
    }

    public static AuthorizeResult failed(String pspRef, String reason) {
        return new AuthorizeResult(pspRef, ChargeStatus.FAILED, reason);
    }
}
