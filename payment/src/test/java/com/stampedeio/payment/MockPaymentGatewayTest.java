package com.stampedeio.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.stampedeio.payment.gateway.AuthorizeRequest;
import com.stampedeio.payment.gateway.ChargeStatus;
import com.stampedeio.payment.gateway.MockPaymentGateway;
import com.stampedeio.payment.gateway.RefundRequest;

/**
 * MockPaymentGateway is the chaos-testing alternative to Stripe. These tests
 * lock in the two guarantees the failure-injection scenarios rely on:
 * failureRate=0.0 always authorizes, failureRate=1.0 always fails.
 */
class MockPaymentGatewayTest {

    private static AuthorizeRequest authReq() {
        UUID cid = UUID.randomUUID();
        return new AuthorizeRequest(cid, 1000L, "USD", "pm_card_visa", cid.toString());
    }

    private static RefundRequest refundReq() {
        UUID cid = UUID.randomUUID();
        return new RefundRequest(cid, "mock_x", 500L, cid.toString());
    }

    @Test
    void failureRateZero_alwaysAuthorizes() {
        MockPaymentGateway gw = new MockPaymentGateway(0.0);
        for (int i = 0; i < 100; i++) {
            assertThat(gw.authorize(authReq()).status()).isEqualTo(ChargeStatus.AUTHORIZED);
        }
        assertThat(gw.name()).isEqualTo("mock");
    }

    @Test
    void failureRateOne_alwaysFails_forChaosTests() {
        MockPaymentGateway gw = new MockPaymentGateway(1.0);
        for (int i = 0; i < 100; i++) {
            assertThat(gw.authorize(authReq()).status()).isEqualTo(ChargeStatus.FAILED);
            assertThat(gw.refund(refundReq()).succeeded()).isFalse();
        }
    }
}
