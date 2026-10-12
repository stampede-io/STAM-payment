package com.stampedeio.payment.config;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * STAM-406 / AC5: see STAM-booking's identical class for the full header
 * contract. Producer side (PaymentService.emitOutcome -> payments.events)
 * calls addHeader(); consumer side (PaymentCommandConsumer <- payments.commands)
 * is wired here via setRecordInterceptor.
 */
public final class KafkaCorrelationIdInterceptor implements RecordInterceptor<String, Map<String, Object>> {

    public static final String HEADER = "correlationId";
    private static final String SERVICE = "payment";

    public static void addHeader(ProducerRecord<?, ?> record, UUID correlationId) {
        record.headers().add(HEADER, correlationId.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public ConsumerRecord<String, Map<String, Object>> intercept(
            ConsumerRecord<String, Map<String, Object>> record, Consumer<String, Map<String, Object>> consumer) {
        Header header = record.headers().lastHeader(HEADER);
        if (header != null) {
            MDC.put(HEADER, new String(header.value(), StandardCharsets.UTF_8));
        }
        MDC.put("service", SERVICE);
        return record;
    }

    @Override
    public void success(ConsumerRecord<String, Map<String, Object>> record,
                         Consumer<String, Map<String, Object>> consumer) {
        clear();
    }

    @Override
    public void failure(ConsumerRecord<String, Map<String, Object>> record, Exception exception,
                         Consumer<String, Map<String, Object>> consumer) {
        clear();
    }

    private void clear() {
        MDC.remove(HEADER);
        MDC.remove("service");
    }
}
