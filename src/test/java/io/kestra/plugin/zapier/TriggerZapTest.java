package io.kestra.plugin.zapier;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class TriggerZapTest {

    @Inject
    private RunContextFactory runContextFactory;

    private WireMockServer wireMockServer;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
        WireMock.configureFor("localhost", wireMockServer.port());
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @Test
    void shouldPostToZapierAndReturnSuccess() throws Exception {
        stubFor(post(urlEqualTo("/hooks/catch/123/456"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("{\"status\": \"success\"}")
                .withHeader("Content-Type", "application/json")
                .withHeader("X-Request-Id", "req-abc-123")));

        var runContext = runContextFactory.of(Map.of());

        var task = TriggerZap.builder()
            .url(new Property<>("http://localhost:" + wireMockServer.port() + "/hooks/catch/123/456"))
            .content(new Property<>(Map.of("event", "test", "data", "hello")))
            .build();

        var output = task.run(runContext);

        assertThat(output.getStatus(), is(200));
        assertThat(output.getBody(), containsString("success"));
        assertThat(output.getAttemptId(), is("req-abc-123"));

        verify(postRequestedFor(urlEqualTo("/hooks/catch/123/456"))
            .withHeader("Content-Type", containing("application/json")));
    }

    @Test
    void shouldNotFailOnErrorWhenAllowFailedIsTrue() throws Exception {
        stubFor(post(urlEqualTo("/hooks/catch/error"))
            .willReturn(aResponse()
                .withStatus(500)
                .withBody("Internal Server Error")));

        var runContext = runContextFactory.of(Map.of());

        var task = TriggerZap.builder()
            .url(new Property<>("http://localhost:" + wireMockServer.port() + "/hooks/catch/error"))
            .content(new Property<>(Map.of("test", "data")))
            .allowFailed(Property.ofValue(true))
            .build();

        var output = task.run(runContext);

        assertThat(output.getStatus(), is(500));
        assertThat(output.getBody(), containsString("Internal Server Error"));
    }

    @Test
    void shouldSendCustomHeaders() throws Exception {
        stubFor(post(urlEqualTo("/hooks/catch/headers"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("ok")));

        var runContext = runContextFactory.of(Map.of());

        var task = TriggerZap.builder()
            .url(new Property<>("http://localhost:" + wireMockServer.port() + "/hooks/catch/headers"))
            .content(new Property<>(Map.of("key", "value")))
            .headers(new Property<>(Map.of("X-Custom-Header", "custom-value", "Authorization", "Bearer test-token")))
            .build();

        var output = task.run(runContext);

        assertThat(output.getStatus(), is(200));

        verify(postRequestedFor(urlEqualTo("/hooks/catch/headers"))
            .withHeader("X-Custom-Header", equalTo("custom-value"))
            .withHeader("Authorization", equalTo("Bearer test-token")));
    }
}
