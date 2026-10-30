package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

/** The service exposes health and scrapeable metrics without exposing its configuration or bean inventory. */
@IntegrationTest
class ObservabilityEndpointTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Value("${local.server.port}")
    private int port;

    /** Health is available to check whether the application has started. */
    @Test
    void tc501_healthEndpointReportsApplicationStatus() throws IOException, InterruptedException {
        HttpResponse<String> response = get("/actuator/health");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\":\"UP\"");
    }

    /** Prometheus receives metrics in its text exposition format. */
    @Test
    void tc502_prometheusEndpointExportsJvmMetrics() throws IOException, InterruptedException {
        HttpResponse<String> response = get("/actuator/prometheus");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).contains("text/plain");
        assertThat(response.body()).contains("# TYPE jvm_memory_used_bytes gauge");
        assertThat(response.body())
                .contains("# TYPE orders_outbox_pending gauge")
                .contains("# TYPE orders_outbox_oldest_pending_seconds gauge")
                .contains("# TYPE orders_outbox_published_total counter")
                .contains("# TYPE orders_outbox_publish_failures_total counter")
                .contains("# TYPE orders_outbox_cleaned_total counter")
                .contains("# TYPE orders_payment_consumer_lag gauge");
    }

    /** Operational endpoints with application details are not exposed over HTTP. */
    @Test
    void tc503_environmentEndpointIsNotExposed() throws IOException, InterruptedException {
        assertThat(get("/actuator/env").statusCode()).isEqualTo(404);
    }

    /** Sends a GET to this test's random application port. */
    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
