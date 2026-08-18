package com.stampedeio.payment.gateway;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.stripe.exception.CardException;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.RefundCreateParams;

@Component
@ConditionalOnProperty(name = "payment.gateway", havingValue = "stripe")
public class StripePaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(StripePaymentGateway.class);

    @Override
    public AuthorizeResult authorize(AuthorizeRequest request) {
        PaymentIntentCreateParams.Builder builder = PaymentIntentCreateParams.builder()
                .setAmount(request.amountCents())
                .setCurrency(request.currency().toLowerCase())
                .setConfirm(true)
                .setPaymentMethod(request.paymentMethodId())
                .setAutomaticPaymentMethods(
                        PaymentIntentCreateParams.AutomaticPaymentMethods.builder()
                                .setEnabled(true)
                                .setAllowRedirects(PaymentIntentCreateParams.AutomaticPaymentMethods.AllowRedirects.NEVER)
                                .build())
                .putMetadata("correlation_id", request.correlationId().toString());

        RequestOptions options = RequestOptions.builder()
                .setIdempotencyKey(request.idempotencyKey())
                .build();

        try {
            PaymentIntent intent = PaymentIntent.create(builder.build(), options);
            return mapStatus(intent);
        } catch (CardException e) {
            // Declines land here — decline_code / code identifies the reason.
            String pspRef = e.getStripeError() != null ? e.getStripeError().getPaymentIntent() != null
                    ? e.getStripeError().getPaymentIntent().getId() : null : null;
            log.warn("Stripe declined authorize for correlationId={} code={}",
                    request.correlationId(), e.getCode());
            return AuthorizeResult.failed(pspRef, e.getCode());
        } catch (StripeException e) {
            log.error("Stripe error on authorize for correlationId={}", request.correlationId(), e);
            return AuthorizeResult.failed(null, "stripe_error:" + e.getCode());
        }
    }

    @Override
    public RefundResult refund(RefundRequest request) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("correlation_id", request.correlationId().toString());
        RefundCreateParams.Builder builder = RefundCreateParams.builder()
                .setPaymentIntent(request.pspRef())
                .putAllMetadata(metadata);
        if (request.amountCents() > 0) {
            builder.setAmount(request.amountCents());
        }

        RequestOptions options = RequestOptions.builder()
                .setIdempotencyKey(request.idempotencyKey())
                .build();

        try {
            Refund refund = Refund.create(builder.build(), options);
            if ("succeeded".equals(refund.getStatus()) || "pending".equals(refund.getStatus())) {
                return RefundResult.ok(refund.getId());
            }
            return RefundResult.failed(refund.getFailureReason() != null ? refund.getFailureReason() : refund.getStatus());
        } catch (StripeException e) {
            log.error("Stripe error on refund for correlationId={}", request.correlationId(), e);
            return RefundResult.failed("stripe_error:" + e.getCode());
        }
    }

    @Override
    public String name() {
        return "stripe";
    }

    private AuthorizeResult mapStatus(PaymentIntent intent) {
        String status = intent.getStatus();
        return switch (status) {
            case "succeeded" -> AuthorizeResult.authorized(intent.getId());
            case "requires_action", "requires_confirmation" ->
                    new AuthorizeResult(intent.getId(), ChargeStatus.REQUIRES_ACTION, null);
            default -> AuthorizeResult.failed(intent.getId(), status);
        };
    }
}
