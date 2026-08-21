package com.stampedeio.payment.config;

import jakarta.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import com.stripe.Stripe;

@Configuration
@ConditionalOnProperty(name = "payment.gateway", havingValue = "stripe")
public class StripeConfig {

    private static final Logger log = LoggerFactory.getLogger(StripeConfig.class);

    @Value("${stripe.api-key:}")
    private String apiKey;

    @PostConstruct
    void init() {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException(
                    "payment.gateway=stripe requires stripe.api-key (STRIPE_API_KEY) to be set");
        }
        if (!apiKey.startsWith("sk_test_") && !apiKey.startsWith("rk_test_")) {
            // Guard-rail: this service is only deployed in test mode for now.
            // Remove when live keys are intentionally introduced with additional review.
            throw new IllegalStateException(
                    "stripe.api-key must be a test-mode key (sk_test_* / rk_test_*)");
        }
        Stripe.apiKey = apiKey;
        log.info("Stripe configured in test mode (key prefix={})", apiKey.substring(0, 8));
    }
}
