package com.stampedeio.payment.web;

import java.util.Map;

import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.stampedeio.payment.gateway.AuthorizeResult;
import com.stampedeio.payment.gateway.ChargeStatus;
import com.stampedeio.payment.service.PaymentService;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> charge(@Valid @RequestBody PaymentRequest request) {
        MDC.put("correlationId", request.correlationId().toString());
        try {
            AuthorizeResult result = paymentService.authorizePayment(
                    request.correlationId(),
                    request.aggregateId(),
                    Map.of(
                            "amountCents", request.amountCents(),
                            "currency", request.currency(),
                            "paymentMethodId", request.paymentMethodId()));

            PaymentResponse body = new PaymentResponse(result.pspRef(), result.status(), result.failureReason());
            HttpStatus status = switch (result.status()) {
                case AUTHORIZED -> HttpStatus.CREATED;
                case REQUIRES_ACTION -> HttpStatus.ACCEPTED;
                case FAILED -> HttpStatus.PAYMENT_REQUIRED;
            };
            return ResponseEntity.status(status).body(body);
        } finally {
            MDC.remove("correlationId");
        }
    }

    @PostMapping("/refunds")
    public ResponseEntity<Map<String, Object>> refund(@Valid @RequestBody RefundHttpRequest request) {
        MDC.put("correlationId", request.correlationId().toString());
        try {
            var result = paymentService.refundPayment(
                    request.correlationId(),
                    request.aggregateId(),
                    Map.of(
                            "pspRef", request.pspRef() == null ? "" : request.pspRef(),
                            "originalCorrelationId", request.originalCorrelationId() == null
                                    ? "" : request.originalCorrelationId().toString(),
                            "amountCents", request.amountCents()));
            HttpStatus status = result.succeeded() ? HttpStatus.CREATED : HttpStatus.PAYMENT_REQUIRED;
            return ResponseEntity.status(status).body(Map.of(
                    "refundRef", result.refundRef() == null ? "" : result.refundRef(),
                    "succeeded", result.succeeded(),
                    "failureReason", result.failureReason() == null ? "" : result.failureReason()));
        } finally {
            MDC.remove("correlationId");
        }
    }
}
