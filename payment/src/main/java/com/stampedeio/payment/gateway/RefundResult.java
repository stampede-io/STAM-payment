package com.stampedeio.payment.gateway;

public record RefundResult(
        String refundRef,
        boolean succeeded,
        String failureReason
) {
    public static RefundResult ok(String refundRef) {
        return new RefundResult(refundRef, true, null);
    }

    public static RefundResult failed(String reason) {
        return new RefundResult(null, false, reason);
    }
}
