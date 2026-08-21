package com.stampedeio.payment.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

/**
 * Payment record — AC6: contains only psp_ref, status, amount, currency.
 * Card numbers, CVVs, and any other PAN data are NEVER persisted here;
 * they are handled exclusively by Stripe.
 */
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @Column(name = "correlation_id")
    private UUID correlationId;

    @Column(name = "aggregate_id")
    private UUID aggregateId;

    @Column(name = "psp_ref", length = 120)
    private String pspRef;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Column(nullable = false, length = 8)
    private String currency;

    @Column(name = "failure_reason", length = 200)
    private String failureReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Payment() {
    }

    public Payment(UUID correlationId, UUID aggregateId, long amountCents, String currency) {
        this.correlationId = correlationId;
        this.aggregateId = aggregateId;
        this.amountCents = amountCents;
        this.currency = currency;
        this.status = "PENDING";
    }

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public void markAuthorized(String pspRef) {
        this.pspRef = pspRef;
        this.status = "AUTHORIZED";
        this.failureReason = null;
    }

    public void markFailed(String pspRef, String reason) {
        this.pspRef = pspRef;
        this.status = "FAILED";
        this.failureReason = reason;
    }

    public void markRequiresAction(String pspRef) {
        this.pspRef = pspRef;
        this.status = "REQUIRES_ACTION";
    }

    public void markRefunded() {
        this.status = "REFUNDED";
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getPspRef() {
        return pspRef;
    }

    public String getStatus() {
        return status;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public String getCurrency() {
        return currency;
    }

    public String getFailureReason() {
        return failureReason;
    }
}
