# Integration testing with Testcontainers: GraphQL, PostgreSQL and Kafka

[![CI](https://github.com/niranjankagri/integration-testing-testcontainers/actions/workflows/ci.yml/badge.svg)](https://github.com/niranjankagri/integration-testing-testcontainers/actions/workflows/ci.yml)

How I test an event-driven service against its **real** dependencies: every test run starts PostgreSQL and Kafka in
Docker with **Testcontainers**, and the tests talk to the service the way its clients do, over **GraphQL** and
**Kafka**. Java 21, Spring Boot 4.1, JUnit 5, Awaitility.

The service is the system under test and deliberately small (a few hundred lines): an order service with a GraphQL
API, a PostgreSQL table, an `OrderPlaced` event it publishes and a `PaymentCompleted` event it consumes. The point of
the repository is the tests.

**Why the app is in this repository:** the tests run against their own copy, started fresh for every run, with its
own database and broker. No shared test environment means no outages, no test data shared between runs, and no change
somewhere else that breaks these tests; one clone and `mvnw verify` run everything.

## What is tested

| Test class | What it proves | How |
|---|---|---|
| `DatabaseMigrationTest` (4) | Flyway migrations run on the real PostgreSQL 18; the database itself rejects bad data | `flyway_schema_history`, check constraints, unique event ids |
| `OrderGraphQlApiTest` (6) | queries, the mutation, validation errors, schema rejections, data isolation | `HttpGraphQlTester` over real HTTP |
| `OrderPlacedEventTest` (2) | every placed order publishes `OrderPlaced`, keyed by order id; an invalid order publishes nothing | a probe consumer on the real topic |
| `PaymentEventsTest` (7) | payment results update orders; duplicates are applied once; dead-lettered events can be replayed after missing orders appear | events sent with `KafkaTemplate`, Awaitility instead of sleeps |
| `KafkaConfigTest` (3) | which failures are retried and which go straight to the dead-letter topic | unit test, no containers |
| `ObservabilityEndpointTest` (3) | health and Prometheus endpoints work; configuration details are not exposed | real HTTP requests to the random application port |

**25 tests** cover database migrations, the GraphQL API, Kafka event processing, and the operational endpoints.

The service also exposes **health and Prometheus metrics** through Spring Boot Actuator, with only the health and
scrape endpoints available over HTTP.

## Local Prometheus, Grafana, and Tempo

The checked-in observability stack scrapes the running service every 15 seconds, stores traces in Tempo, and
provisions a Grafana dashboard for payment outcomes, delivery and recovery failures, dead-letter activity, and HTTP
request rate. Spring Boot records HTTP spans and propagates trace context through observed Kafka producer and
consumer operations. Prometheus, Grafana, and Tempo ports bind to localhost only. The container images are pinned in
`compose.observability.yml`.

Start the stack from the repository root, then start the service. Set the sampling probability to `1.0` for a local
demo so every request is searchable; the service default is `0.1`:

```
docker compose -f compose.observability.yml up -d
set TRACE_SAMPLING_PROBABILITY=1.0
mvnw.cmd spring-boot:test-run
```

Open [Grafana](http://localhost:3000) and sign in with the local default credentials `admin` / `admin`; Grafana
prompts you to set a new password. The **Order Service / Order Service Overview** dashboard is provisioned
automatically. To inspect traces, open **Explore**, select **Tempo**, and search for the `orderservice` service.
Prometheus is available at [localhost:9090](http://localhost:9090); check **Status / Targets** and confirm
`order-service` is `UP` if the dashboard has no data. Stop the stack with:

```
docker compose -f compose.observability.yml down
```

The Prometheus and Grafana data volumes survive `down`; remove them with `docker compose -f compose.observability.yml
down -v` when you want to reset local data. This stack is for local development and does
not add the observability ports to the application itself. Override `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` if Tempo
is hosted somewhere other than `http://localhost:4318/v1/traces`.

## Why real containers instead of mocks or an in-memory database

- **H2 is not PostgreSQL.** Syntax, types, constraints and `ON CONFLICT` differ; a migration that passes on H2 can
  fail in production. Here the tests run on PostgreSQL 18, the version the service targets, pinned.
- **An embedded or mocked broker hides the hard parts:** partitions, keys, ordering, redelivery, offsets, dead-letter
  topics. Those are where event-driven services break.
- **A shared test environment is the classic source of flaky tests:** data left by other runs, outages, version drift.
  A container per run starts empty, at a pinned version.

## Testing asynchronous behaviour without sleeps

Events are processed in the background, so a test cannot assert right after sending. Each check **polls until the
expected state appears** (Awaitility) and fails after a timeout, so it is as fast as the system allows and never
depends on a guessed sleep.

Proving that something does **not** happen needs a bound too. The tests send a later event that must be processed
after the one under test (same key, same partition) and wait for that: once it is done, the earlier one has been
handled. See `aRedeliveredEventIsAppliedOnlyOnce`.

## Event handling the tests pin down

| Situation | Expected behaviour | Test |
|---|---|---|
| The same event delivered twice (Kafka delivers at least once) | applied once, by event id, even if the copy differs | `aRedeliveredEventIsAppliedOnlyOnce` |
| Broken JSON | straight to `payments.completed.DLT`, no retries; later events for the same order still processed | `brokenJsonGoesToTheDeadLetterTopicAndDoesNotBlockLaterEvents` |
| Required field missing | straight to the dead-letter topic | `anEventWithoutRequiredFieldsGoesToTheDeadLetterTopic` |
| Unknown order (maybe not committed yet) | retried, then dead-lettered; nothing marked as processed, so a replay can still work | `anEventForAnUnknownOrderIsRetriedThenDeadLettered` |
| Order placed | `OrderPlaced` published only after the database commit, keyed by order id | `placingAnOrderPublishesOrderPlacedKeyedByTheOrderId` |

## What the tests found

- **The dead-letter topic name changed under me.** Spring Kafka 4 names it `<topic>-dlt`; earlier versions used
  `<topic>.DLT`. With the default, failed events went to a topic nobody watched, and three tests timed out. The
  service now names the topic explicitly instead of relying on a framework default.
- **A gap in my own tests.** As a mutation check I removed the "do not retry broken JSON" rule: every container test
  still passed, because the event still ends up in the dead-letter topic, just a second later. `KafkaConfigTest` now
  checks the retry rules directly. A second mutation (removing the duplicate check) was caught by
  `aRedeliveredEventIsAppliedOnlyOnce`.
- **GraphQL null propagation.** `placeOrder` returns `Order!`; when it fails, the whole `data` becomes null, not just
  the field. The test asserts exactly that.

## GraphQL testing notes

- GraphQL answers HTTP 200 even when a request fails, so every test checks the `errors` array, never only the
  status.
- Invalid input returns a `BAD_REQUEST` error that names each broken rule; nothing is stored (checked in the
  database).
- Missing required arguments and unknown fields are rejected by the schema before any code runs.

## Health and Prometheus metrics

The service exposes `/actuator/health` for a basic availability check and `/actuator/prometheus` for scraping. The
Prometheus registry exports standard JVM and HTTP server measurements, plus the bounded
`orders.payment.events` counter with outcomes `paid`, `cancelled`, `duplicate`, `ignored`, `delivery_failed`,
`dead_lettered` and `recovery_failed`. It never tags a metric with a customer, order id or event id, so traffic cannot
create unbounded time series. Integration tests also prove a dead-lettered payment can be replayed after its order
becomes available.

Only `health` and `prometheus` are exposed over HTTP; endpoints such as `/actuator/env` remain unavailable. The
integration tests check both scrape format and the endpoint allowlist. With the service running locally:

```
curl http://localhost:8080/actuator/health
curl http://localhost:8080/actuator/prometheus
```

## Running it (Windows cmd)

Needs Java 21 or later and Docker Desktop.

```
cd /d D:\Data\Work\portfolio-projects\integration-testing-testcontainers
mvnw.cmd verify                       all 25 tests (pulls postgres:18-alpine and apache/kafka-native:4.1.1 once)
mvnw.cmd test -Dtest=PaymentEventsTest
mvnw.cmd spring-boot:test-run         the service with the same containers; GraphiQL at http://localhost:8080/graphiql
```

## Project structure

```
src/main/java/com/niranjankagri/orders/
  order/      Order, OrderRepository, OrderService, OrderGraphQlController, PlaceOrderInput, GraphQlErrors
  events/     Topics, OrderPlaced, PaymentCompleted, OrderEventPublisher, PaymentListener, KafkaConfig
src/main/resources/
  graphql/schema.graphqls    db/migration/V1__create_orders.sql    application.properties
observability/
  prometheus/prometheus.yml   tempo/tempo.yml   grafana/provisioning/   grafana/dashboards/
compose.observability.yml
src/test/java/com/niranjankagri/orders/
  TestcontainersConfiguration   pinned PostgreSQL and Kafka containers (@ServiceConnection)
  IntegrationTest               one annotation, one cached context, containers started once
  KafkaProbe                    reads a topic from "now" on, outside any consumer group
  *Test                         the tests listed above
  TestOrderServiceApplication   runs the service locally with the containers
```

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml): `mvnw verify` on GitHub's Ubuntu runner, which has Docker,
so the containers start exactly as locally. A table of test classes, counts and times goes into the job summary. The JUnit XML is kept as the
**junit-integration** artifact, which [Automation Coverage & Quality Intelligence](https://github.com/niranjankagri/automation-coverage-quality-intelligence) collects.

CI also runs every Friday at 23:15 UTC without a code change, so a broken dependency or a change outside the repository shows up within a week. Dependabot proposes dependency and GitHub Actions updates once a month (minor and patch updates grouped, major versions as separate pull requests).

`main` is protected by a ruleset: every change goes through a pull request, and this CI job must pass before it is merged.

## Limits

- `OrderPlaced` is sent after the commit; a crash between commit and send would lose it. Production would use a
  transactional outbox (the event stored in the same transaction and relayed). The trade-off is documented in the code.
- One service. Contract tests between producer and consumer teams (for example with Pact or a schema registry) are
  shown in my [pact-contract-testing](https://github.com/niranjankagri/pact-contract-testing) repository.

See [docs/framework-design.md](docs/framework-design.md) and [docs/interview-questions.md](docs/interview-questions.md).

## License

[MIT](LICENSE)
