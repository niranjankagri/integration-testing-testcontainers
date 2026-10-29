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
| `DatabaseMigrationTest` (6) | Flyway migrations run on the real PostgreSQL 18; the database itself rejects bad data; outbox claims recover and retention preserves pending rows | `flyway_schema_history`, check constraints, unique event ids |
| `OrderGraphQlApiTest` (6) | queries, the mutation, validation errors, schema rejections, data isolation | `HttpGraphQlTester` over real HTTP |
| `OrderPlacedEventTest` (3) | the order and outbox row commit together; the relay publishes `OrderPlaced` keyed by order id | real PostgreSQL transaction and a probe consumer on Kafka |
| `PaymentEventsTest` (7) | payment results update orders; duplicates are applied once; dead-lettered events can be replayed after missing orders appear | events sent with `KafkaTemplate`, Awaitility instead of sleeps |
| `KafkaConfigTest` (3) | which failures are retried and which go straight to the dead-letter topic | unit test, no containers |
| `ObservabilityEndpointTest` (3) | health and Prometheus endpoints work; configuration details are not exposed | real HTTP requests to the random application port |
| `ConsumerLagTest` (1) | the consumer-lag gauge counts uncommitted payment events while the listener is stopped, and returns to zero after the backlog is processed | the listener container paused and resumed; three payments sent while paused |
| `OutboxCrashRecoveryTest` (1) | a relay that dies after Kafka acknowledged an event, before the database mark: a restarted instance sends the event again with the same id once the claim lease expires, and marks it published | two service instances one after the other on their own PostgreSQL and Kafka; the crash is an `Error` thrown from `markPublished` |

**30 tests** cover database migrations, the GraphQL API, Kafka event processing, crash recovery of the outbox, and the
operational endpoints.

The service also exposes **health and Prometheus metrics** through Spring Boot Actuator, with only the health and
scrape endpoints available over HTTP.

## Local Prometheus, Grafana, and Tempo

The checked-in observability stack scrapes the running service every 15 seconds, stores traces in Tempo, and
provisions a Grafana dashboard for payment and outbox outcomes, delivery and recovery failures, dead-letter activity,
published outbox cleanup, payment consumer lag, and HTTP request rate. Spring Boot records HTTP spans and propagates trace context through observed Kafka producer and
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
`order-service` is `UP` if the dashboard has no data.

Prometheus also evaluates the rules in `observability/prometheus/rules/order-service.yml`. Check
[Alerts](http://localhost:9090/alerts) for service availability, dead-lettered payments, recovery publish failures,
an outbox event waiting longer than one minute, and a payment consumer more than 100 events behind for five minutes;
[Rules](http://localhost:9090/rules) shows their evaluation state. The rules have unit tests in
`observability/prometheus/tests`, which CI runs with `promtool` (each alert fires when it should and stays quiet
otherwise). Alert notifications are not configured in this local stack. Stop the stack with:

```
docker compose -f compose.observability.yml down
```

The Prometheus and Grafana data volumes survive `down`; remove them with `docker compose -f compose.observability.yml
down -v` when you want to reset local data. This stack is for local development and does
not add the observability ports to the application itself. Override `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` if Tempo
is hosted somewhere other than `http://localhost:4318/v1/traces`.
Only traces go out over OTLP: metrics reach Prometheus by scraping, so the OTLP metrics push that the OpenTelemetry
starter enables by default is switched off (`management.otlp.metrics.export.enabled=false`). The tests switch off
trace export too, because no collector runs in CI.

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
| Order placed | order and `OrderPlaced` outbox row commit together; the relay publishes it keyed by order id | `placingAnOrderPublishesOrderPlacedKeyedByTheOrderId`, `rollingBackAnOrderAlsoRollsBackItsOutboxEvent` |

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
`dead_lettered` and `recovery_failed`. Outbox metrics report pending event count, oldest pending event age, successful
publishes, failed relay attempts, and rows removed by retention. They never tag a metric with a customer, order id or
event id, so traffic cannot create unbounded time series. Integration tests also prove a dead-lettered payment can be
replayed after its order becomes available; [docs/dead-letter-replay.md](docs/dead-letter-replay.md) is the procedure
to inspect and replay dead-lettered events with Kafka's command-line tools.

`orders.payment.consumer.lag` is the payment consumer group's lag: log-end offsets of `payments.completed` minus the
group's committed offsets, read from the broker at each scrape. Kafka's own client metric
(`kafka_consumer_fetch_manager_records_lag_max`) is also exported, but it is measured while the consumer fetches, so a
stuck or paused listener stops reporting it; the broker-side gauge keeps rising.

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
mvnw.cmd verify                       all 30 tests (pulls postgres:18-alpine and apache/kafka-native:4.1.1 once)
mvnw.cmd test -Dtest=PaymentEventsTest
mvnw.cmd spring-boot:test-run         the service with the same containers; GraphiQL at http://localhost:8080/graphiql
```

## Project structure

```
src/main/java/com/niranjankagri/orders/
  order/      Order, OrderRepository, OrderService, OrderGraphQlController, PlaceOrderInput, GraphQlErrors
  events/     Topics, OrderPlaced, PaymentCompleted, OutboxEventStore, OrderOutboxRelay, PaymentListener, KafkaConfig,
              ConsumerLagMetrics
src/main/resources/
  graphql/schema.graphqls    db/migration/V1__create_orders.sql    application.properties
observability/
  prometheus/prometheus.yml   prometheus/rules/   prometheus/tests/ (alert rule tests)   tempo/tempo.yml
  grafana/provisioning/       grafana/dashboards/
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
A second job checks the Prometheus configuration and runs the alert rule tests with `promtool` from the same image the
local stack uses.

CI also runs every Friday at 23:15 UTC without a code change, so a broken dependency or a change outside the repository shows up within a week. Dependabot proposes dependency and GitHub Actions updates once a month (minor and patch updates grouped, major versions as separate pull requests).

`main` is protected by a ruleset: every change goes through a pull request, and this CI job must pass before it is merged.

## Limits

- `OrderPlaced` uses an at-least-once transactional outbox. A crash after Kafka accepts a message but before the
  database marks it published can cause a duplicate; the stable event id lets consumers deduplicate it. PostgreSQL
  claim leases let multiple relay instances work concurrently, and only the oldest unpublished event per order can
  be claimed so retries do not let later events overtake it. A crashed worker's claim becomes available after 30 seconds
  (`ORDERS_OUTBOX_CLAIM_LEASE_SECONDS`); `OutboxCrashRecoveryTest` proves the takeover and the duplicate with a real
  restart.
- Published outbox rows are removed in batches after 30 days by default. Set `ORDERS_OUTBOX_RETENTION_DAYS` to match
  the service's audit policy; archive events elsewhere if they must be retained longer. Pending rows are never removed.
- One service. Contract tests between producer and consumer teams (for example with Pact or a schema registry) are
  shown in my [pact-contract-testing](https://github.com/niranjankagri/pact-contract-testing) repository.

See [docs/framework-design.md](docs/framework-design.md) and [docs/interview-questions.md](docs/interview-questions.md).

## License

[MIT](LICENSE)
