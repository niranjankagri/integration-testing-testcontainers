package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * Proves the database connection path notices network faults and works again after they are removed.
 * PostgreSQL itself remains the same real container used by the application integration tests.
 */
@IntegrationTest
class DatabaseResilienceTest {

    private static final int PROXY_PORT = 8666;
    private static final int SOCKET_TIMEOUT_SECONDS = 2;

    @Autowired
    PostgreSQLContainer<?> postgres;

    private ToxiproxyContainer toxiproxy;
    private Proxy postgresProxy;

    @BeforeEach
    void startProxy() throws Exception {
        int postgresPort = postgres.getMappedPort(5432);
        Testcontainers.exposeHostPorts(postgresPort);

        toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0"))
                .withAccessToHost(true);
        toxiproxy.start();

        ToxiproxyClient control = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        postgresProxy = control.createProxy(
                "postgres",
                "0.0.0.0:" + PROXY_PORT,
                "host.testcontainers.internal:" + postgresPort);
    }

    @AfterEach
    void stopProxy() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
    }

    @Test
    void tc601_latencyAndConnectionLossAreObservableAndRecoverable() throws Exception {
        try (Connection connection = openProxiedConnection()) {
            assertThat(selectOne(connection)).isEqualTo(1);

            postgresProxy.toxics().latency("database-latency", ToxicDirection.DOWNSTREAM, 300);
            Instant started = Instant.now();
            assertThat(selectOne(connection)).isEqualTo(1);
            assertThat(Duration.between(started, Instant.now()).toMillis()).isGreaterThanOrEqualTo(250);
            postgresProxy.toxics().get("database-latency").remove();

            postgresProxy.disable();
            assertThatThrownBy(() -> selectOne(connection)).isInstanceOf(SQLException.class);
            postgresProxy.enable();
        }

        // A fresh connection proves that recovery reaches PostgreSQL after the network is restored.
        try (Connection recovered = openProxiedConnection()) {
            assertThat(selectOne(recovered)).isEqualTo(1);
        }
    }

    private Connection openProxiedConnection() throws SQLException {
        String url = "jdbc:postgresql://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT) + "/"
                + postgres.getDatabaseName() + "?connectTimeout=3&socketTimeout=" + SOCKET_TIMEOUT_SECONDS;
        return DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
    }

    private int selectOne(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT 1")) {
            result.next();
            return result.getInt(1);
        }
    }
}
