package com.stampedeio.payment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.nio.charset.StandardCharsets;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.stampedeio.payment.service.PaymentService;
import com.stampedeio.payment.web.StripeWebhookController;

@ExtendWith(MockitoExtension.class)
class StripeWebhookControllerTest {

    private static final String SECRET = "whsec_test_secret_for_it";

    @Mock
    private PaymentService paymentService;

    private MockMvc mvc;

    private static final String INTENT_PAYLOAD =
            "{\"id\":\"evt_test_1\",\"type\":\"payment_intent.succeeded\","
                    + "\"api_version\":\"2024-04-10\","
                    + "\"data\":{\"object\":{\"id\":\"pi_test_ok\",\"object\":\"payment_intent\","
                    + "\"amount\":5000,\"currency\":\"usd\",\"status\":\"succeeded\"}}}";

    @BeforeEach
    void setUp() {
        StripeWebhookController controller = new StripeWebhookController(paymentService, SECRET);
        mvc = standaloneSetup(controller).build();
    }

    @Test
    void ac3_validSignature_isAccepted_andHandlerIsInvoked() throws Exception {
        long ts = System.currentTimeMillis() / 1000L;
        String sig = stripeSignature(ts, INTENT_PAYLOAD, SECRET);

        mvc.perform(post("/api/v1/payments/webhook")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", sig)
                        .content(INTENT_PAYLOAD))
                .andExpect(status().isOk());

        verify(paymentService, times(1)).applyWebhookOutcome(eq("pi_test_ok"), eq(true), any());
    }

    @Test
    void ac4_tamperedPayload_returns400_andHandlerIsNotInvoked() throws Exception {
        long ts = System.currentTimeMillis() / 1000L;
        String sig = stripeSignature(ts, INTENT_PAYLOAD, SECRET);
        String tampered = INTENT_PAYLOAD.replace("5000", "9999");

        mvc.perform(post("/api/v1/payments/webhook")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", sig)
                        .content(tampered))
                .andExpect(status().isBadRequest());

        verify(paymentService, never()).applyWebhookOutcome(anyString(), any(Boolean.class), any());
    }

    @Test
    void ac4_missingSignature_returns400() throws Exception {
        mvc.perform(post("/api/v1/payments/webhook")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(INTENT_PAYLOAD))
                .andExpect(status().isBadRequest());

        verify(paymentService, never()).applyWebhookOutcome(anyString(), any(Boolean.class), any());
    }

    @Test
    void ac4_wrongSecret_returns400() throws Exception {
        long ts = System.currentTimeMillis() / 1000L;
        String sig = stripeSignature(ts, INTENT_PAYLOAD, "whsec_wrong_secret");

        mvc.perform(post("/api/v1/payments/webhook")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Stripe-Signature", sig)
                        .content(INTENT_PAYLOAD))
                .andExpect(status().isBadRequest());

        verify(paymentService, never()).applyWebhookOutcome(anyString(), any(Boolean.class), any());
    }

    private static String stripeSignature(long timestamp, String payload, String secret) throws Exception {
        String signedPayload = timestamp + "." + payload;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hmac = mac.doFinal(signedPayload.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(hmac.length * 2);
        for (byte b : hmac) {
            hex.append(String.format("%02x", b));
        }
        return "t=" + timestamp + ",v1=" + hex;
    }
}
