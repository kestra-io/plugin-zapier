package io.kestra.plugin.zapier;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send data to a Zap",
    description = """
        Sends an HTTP request to a Zapier Catch Hook URL to trigger a Zap.
        Supports `POST`, `PUT`, and `GET`; `POST` is the default, JSON content is only sent for `POST` and `PUT`, and the request timeout defaults to 30 seconds."""
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a Zap with a JSON payload",
            full = true,
            code = """
                id: trigger_zap
                namespace: company.team

                tasks:
                  - id: trigger_zap
                    type: io.kestra.plugin.zapier.TriggerZap
                    url: "{{ secret('ZAPIER_WEBHOOK_URL') }}"
                    content:
                      name: "Kestra"
                      event: "workflow_completed"
                      status: "success"
                """
        )
    }
)
public class TriggerZap extends Task implements RunnableTask<TriggerZap.Output> {

    @Schema(
        title = "Zapier Catch Hook URL",
        description = """
            Full Zapier webhook URL to call after rendering.
            Use the Catch Hook URL provided by Zapier, typically `https://hooks.zapier.com/hooks/catch/...`"""
    )
    @NotNull
    private Property<String> url;

    @Schema(
        title = "JSON request content",
        description = """
            Optional map rendered from the task context and serialized as JSON.
            Ignored for `GET` requests because this task only builds a request body for `POST` and `PUT`"""
    )
    private Property<Map<String, Object>> content;

    @Schema(
        title = "HTTP method",
        description = """
            HTTP method used for the webhook call.
            Defaults to `POST`; supported values are `POST`, `PUT`, and `GET`"""
    )
    @Builder.Default
    private Property<HttpMethod> method = Property.ofValue(HttpMethod.POST);

    @Schema(
        title = "Custom HTTP headers",
        description = """
            Optional headers added to the outbound request after rendering.
            If a JSON body is sent and `Content-Type` is not provided, this task sets it to `application/json`"""
    )
    private Property<Map<String, String>> headers;

    @Schema(
        title = "Request timeout",
        description = """
            Maximum time to wait for the Zapier webhook response.
            Defaults to 30 seconds and is applied as the HTTP connect timeout configuration"""
    )
    @Builder.Default
    private Property<Duration> timeout = Property.ofValue(Duration.ofSeconds(30));

    @Schema(
        title = "Allow failed HTTP responses",
        description = """
            When enabled, the task returns output for non-2xx responses instead of failing.
            Defaults to `false`, so HTTP failures raise an exception unless explicitly allowed"""
    )
    @Builder.Default
    private Property<Boolean> allowFailed = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rUrl = runContext.render(url).as(String.class).orElseThrow();
        var rMethod = runContext.render(method).as(HttpMethod.class).orElse(HttpMethod.POST);
        var rTimeout = runContext.render(timeout).as(Duration.class).orElse(Duration.ofSeconds(30));
        var rAllowFailed = runContext.render(allowFailed).as(Boolean.class).orElse(false);

        @SuppressWarnings("unchecked")
        var rContent = runContext.render(content).asMap(String.class, Object.class);

        @SuppressWarnings("unchecked")
        var rHeaders = runContext.render(headers).asMap(String.class, String.class);

        logger.info("Sending {} request to Zapier webhook: {}", rMethod, rUrl);

        var uri = URI.create(rUrl);

        // Build headers map
        Map<String, List<String>> headerMap = new java.util.HashMap<>();
        if (rHeaders != null) {
            rHeaders.forEach((k, v) -> headerMap.put(k, List.of(v)));
        }

        // Build the request body
        HttpRequest.RequestBody body = null;
        if (rContent != null && (rMethod == HttpMethod.POST || rMethod == HttpMethod.PUT)) {
            if (!headerMap.containsKey("Content-Type")) {
                headerMap.put("Content-Type", List.of("application/json"));
            }
            body = HttpRequest.JsonRequestBody.builder()
                .content(rContent)
                .build();
        }

        var httpRequest = HttpRequest.of(uri, rMethod.name(), body, headerMap);

        var httpConfig = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder()
                .connectTimeout(Property.ofValue(rTimeout))
                .build())
            .allowFailed(Property.ofValue(rAllowFailed))
            .build();

        try (var client = new HttpClient(runContext, httpConfig)) {
            var response = client.request(httpRequest, String.class);

            var statusCode = response.getStatus().getCode();
            var responseBody = response.getBody();

            // Extract attempt/request ID from response headers
            String attemptId = null;
            if (response.getHeaders() != null) {
                var headersMap = response.getHeaders().map();
                attemptId = extractHeaderValue(headersMap, "X-Attempt-Id");
                if (attemptId == null) {
                    attemptId = extractHeaderValue(headersMap, "X-Request-Id");
                }
            }

            logger.info("Zapier responded with status: {}, body: {}", statusCode, responseBody);
            if (attemptId != null) {
                logger.info("Zapier Attempt/Request ID: {}", attemptId);
            }

            return Output.builder()
                .status(statusCode)
                .body(responseBody != null ? responseBody : "")
                .attemptId(attemptId)
                .build();

        } catch (HttpClientResponseException e) {
            var response = e.getResponse();
            var statusCode = response != null ? response.getStatus().getCode() : 0;
            var responseBody = response != null && response.getBody() != null ? response.getBody().toString() : "";

            String attemptId = null;
            if (response != null && response.getHeaders() != null) {
                var headersMap = response.getHeaders().map();
                attemptId = extractHeaderValue(headersMap, "X-Attempt-Id");
                if (attemptId == null) {
                    attemptId = extractHeaderValue(headersMap, "X-Request-Id");
                }
            }

            if (rAllowFailed) {
                logger.warn("Zapier request failed with status {} but allowFailed is true", statusCode);
                return Output.builder()
                    .status(statusCode)
                    .body(responseBody)
                    .attemptId(attemptId)
                    .build();
            }

            throw new Exception("Zapier webhook request failed with status " + statusCode + ": " + responseBody, e);
        }
    }

    private String extractHeaderValue(Map<String, List<String>> headers, String headerName) {
        for (var entry : headers.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(headerName) && !entry.getValue().isEmpty()) {
                return entry.getValue().getFirst();
            }
        }
        return null;
    }

    public enum HttpMethod {
        POST,
        PUT,
        GET
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "HTTP response status code",
            description = """
                Status code returned by the Zapier webhook endpoint"""
        )
        private final Integer status;

        @Schema(
            title = "Raw response body",
            description = """
                Response body returned by Zapier as a string"""
        )
        private final String body;

        @Schema(
            title = "Zapier attempt or request ID",
            description = """
                `X-Attempt-Id` or `X-Request-Id` extracted from the response headers when Zapier provides one.
                Returns `null` when neither header is present"""
        )
        private final String attemptId;
    }
}
