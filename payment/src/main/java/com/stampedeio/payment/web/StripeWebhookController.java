package com.stampedeio.payment.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.stampedeio.payment.service.PaymentService;
import com.stripe.exception.EventDataObjectDeserializationException;
import com.stripe.exception.SignatureVerificationException;
import com.stripe.model.Event;
import com.stripe.model.EventDataObjectDeserializer;
import com.stripe.model.PaymentIntent;
import com.stripe.model.StripeObject;
import com.stripe.net.Webhook;

/**
 * Stripe webhook receiver. Every request MUST pass HMAC-SHA256 signature
 * verification (Webhook.constructEvent) before any payload is trusted or
 * persisted (AC3, AC4). We deliberately consume the raw body as a string,
 * because Stripe's signature is computed over the exact bytes Stripe sent —
 * any Jackson round-trip would break verification.
 */
@RestController
@RequestMapping("/api/v1/payments/webhook")
public class StripeWebhookController {

    private static final Logger log = LoggerFactory.getLogger(StripeWebhookController.class);

    private final PaymentService paymentService;
    private final String webhookSecret;

    public StripeWebhookController(PaymentService paymentService,
                                   @Value("${stripe.webhook-secret:}") String webhookSecret) {
        this.paymentService = paymentService;
        this.webhookSecret = webhookSecret;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> receive(@RequestBody String payload,
                                          @RequestHeader(value = "Stripe-Signature", required = false)
                                          String signature) {
        if (webhookSecret == null || webhookSecret.isBlank()) {
            log.error("Stripe webhook received but stripe.webhook-secret is not configured — refusing");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("webhook_not_configured");
        }
        if (signature == null || signature.isBlank()) {
            log.warn("Stripe webhook missing Stripe-Signature header — rejecting");
            return ResponseEntity.badRequest().body("missing_signature");
        }

        Event event;
        try {
            event = Webhook.constructEvent(payload, signature, webhookSecret);
        } catch (SignatureVerificationException e) {
            log.warn("Stripe webhook signature verification failed: {}", e.getMessage());
            return ResponseEntity.badRequest().body("invalid_signature");
        } catch (RuntimeException e) {
            // Malformed JSON / other parse errors — also reject with 400, do NOT process.
            log.warn("Stripe webhook payload could not be parsed: {}", e.getMessage());
            return ResponseEntity.badRequest().body("invalid_payload");
        }

        EventDataObjectDeserializer deserializer = event.getDataObjectDeserializer();
        StripeObject data = deserializer.getObject().orElse(null);
        if (data == null) {
            // API-version drift between the Stripe SDK and the sender's event.
            // deserializeUnsafe forces deserialization; we've already validated the
            // signature above so the payload is trusted.
            try {
                data = deserializer.deserializeUnsafe();
            } catch (EventDataObjectDeserializationException e) {
                log.warn("Stripe webhook: could not deserialize event data: {}", e.getMessage());
                return ResponseEntity.badRequest().body("invalid_payload");
            }
        }
        if (!(data instanceof PaymentIntent intent)) {
            // We ack unknown event types so Stripe doesn't retry them forever.
            log.info("Stripe webhook: ignoring event type={}", event.getType());
            return ResponseEntity.ok("ignored");
        }

        switch (event.getType()) {
            case "payment_intent.succeeded" ->
                    paymentService.applyWebhookOutcome(intent.getId(), true, null);
            case "payment_intent.payment_failed" ->
                    paymentService.applyWebhookOutcome(
                            intent.getId(), false,
                            intent.getLastPaymentError() != null ? intent.getLastPaymentError().getCode() : "unknown");
            default -> log.info("Stripe webhook: ignoring event type={}", event.getType());
        }
        return ResponseEntity.ok("");
    }
}
