package com.stampedeio.payment.gateway;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "payment.gateway", havingValue = "mock", matchIfMissing = true)
public class MockPaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(MockPaymentGateway.class);

    private final double failureRate;

    public MockPaymentGateway(@Value("${payment.failure-rate:0.0}") double failureRate) {
        this.failureRate = failureRate;
        log.info("MockPaymentGateway active (failureRate={})", failureRate);
    }

    @Override
    public AuthorizeResult authorize(AuthorizeRequest request) {
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            return AuthorizeResult.failed("mock_" + UUID.randomUUID(), "chaos_injected_failure");
        }
        return AuthorizeResult.authorized("mock_" + UUID.randomUUID());
    }

    @Override
    public RefundResult refund(RefundRequest request) {
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            return RefundResult.failed("chaos_injected_refund_failure");
        }
        return RefundResult.ok("mockrf_" + UUID.randomUUID());
    }

    @Override
    public String name() {
        return "mock";
    }
}
