package com.stampedeio.payment.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "idempotency_keys")
public class IdempotencyKey {

    @Id
    @Column(name = "correlation_id")
    private UUID correlationId;

    @Column(name = "command_type", nullable = false, length = 80)
    private String commandType;

    @Column(nullable = false, length = 40)
    private String outcome;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected IdempotencyKey() {
    }

    public IdempotencyKey(UUID correlationId, String commandType, String outcome) {
        this.correlationId = correlationId;
        this.commandType = commandType;
        this.outcome = outcome;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public String getCommandType() {
        return commandType;
    }

    public String getOutcome() {
        return outcome;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
