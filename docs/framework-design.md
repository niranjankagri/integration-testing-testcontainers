# Framework design

Why the project is built the way it is. The README says what it does and how to run it.

## 1. Goals

1. Show how to test an event-driven service at the integration level: against its real database and message broker,
   through the interfaces its clients use (GraphQL over HTTP, Kafka topics).
2. Make the tests reliable: no shared environment, no sleeps, no order dependence, the same result locally and in CI.
3. Cover the cases where event-driven services really break: duplicates, broken messages, events that arrive too
   early, ordering, and what happens after a failure.

## 2. The parts

| Part | Role | Tools |
|---|---|---|
| `order/` | the order model, the GraphQL API, input validation and error mapping | Spring for GraphQL, Bean Validation, Spring Data JDBC |
| `events/` | `OrderPlaced` out after commit, `PaymentCompleted` in with deduplication and a dead-letter topic | Spring Kafka, Jackson 3 |
| `db/migration` | the schema, including check constraints | Flyway, PostgreSQL 18 |
| `TestcontainersConfiguration` | PostgreSQL and Kafka in Docker, wired with `@ServiceConnection` | Testcontainers 2 |
| tests | API, events, migrations, retry rules | JUnit 5, `HttpGraphQlTester`, AssertJ, Awaitility |

## 3. The service is small on purpose

It has exactly the features that make testing interesting: a schema with validation, a database with constraints,
one event out (with a timing rule: after commit) and one event in (with idempotency, retries and a dead-letter topic).
Everything else (authentication, more entities, a real payment service) would add code without adding test ideas.

## 4. Test environment

- **One context, containers once.** Every test class uses the same `@IntegrationTest` annotation, so Spring caches one
  application context and Testcontainers starts each container once per run. The first test class pays about 13
  seconds; the rest reuse it.
- **`@ServiceConnection`** passes container addresses to Spring Boot: no URLs or ports in any configuration file.
- **Pinned images** (`postgres:18-alpine`, `apache/kafka-native:4.1.1`): `latest` would let a new release change
  results without a change here. The native Kafka image starts in about a second.
- **Data isolation without cleanup:** each test creates its own customer (`customer-<uuid>`) and its own orders, so
  tests never see each other's data, in any order, and could run in parallel. No truncate between tests.
- **`TestOrderServiceApplication`** runs the service with the same containers for manual exploration (GraphiQL).

## 5. Asynchronous assertions

- **Awaitility** polls until a condition holds and fails after a timeout (15 to 20 seconds, generous because it is
  only reached on failure). A passing check returns as soon as the state appears.
- **`KafkaProbe`** reads a topic like a downstream consumer would: assigned to every partition, positioned at the end
  when opened (so it sees only records produced by the test), and outside any consumer group (it never takes
  messages from the service).
- **Proving absence** needs a bound: send a later message that is processed after the one under test (same key, same
  partition), wait for it, then assert the earlier one had no effect. Across partitions (`anInvalidOrderPublishesNothing`)
  this is a strong indication rather than a strict guarantee, and the test says so.

## 6. Event handling decisions

- **Publish after commit** (`@TransactionalEventListener`): never announce an order that rolled back. The trade-off
  (a crash after commit loses the event) is named in the code; a transactional outbox would remove it.
- **Keys:** events are keyed by order id, so all events of one order land in one partition and keep their order.
- **Idempotency:** the event id goes into `processed_events` in the same transaction as the order change. A duplicate
  is skipped; if processing fails, the mark rolls back with it, so a retry or a later replay still works.
- **Retries:** an unknown order is retried twice (500 ms apart), because the event may have overtaken the order's
  commit. Broken JSON and incomplete events are not retried: retrying cannot fix them.
- **Dead-letter topic:** after the retries the record goes to `payments.completed.DLT` with headers naming the
  exception and its cause, and the partition moves on: one poison message cannot block the others.
- **Explicit names:** the dead-letter topic is named in code. The default changed between Spring Kafka versions
  (`.DLT` before 4.0, `-dlt` since), which the tests caught.
- **State rules:** a payment result only applies to a `PLACED` order; anything else is logged and ignored.

## 7. GraphQL testing

- `HttpGraphQlTester` sends real HTTP requests to the running app (random port), so routing, JSON and error mapping
  are part of the test.
- Errors are checked by classification and message: invalid input is `BAD_REQUEST` with the broken rules listed, and
  the database is checked afterwards to prove nothing was stored.
- Schema-level rejections (missing required argument, unknown field) are tested too: they happen before any code
  runs and protect every resolver.
- A failing non-null field nulls its parent (`data: null`); the test asserts this GraphQL rule explicitly.

## 8. Proving the tests

Two mutation checks, run by hand:

| Mutation | Result |
|---|---|
| Remove the duplicate check in `PaymentListener` | caught by `aRedeliveredEventIsAppliedOnlyOnce` |
| Retry broken JSON and invalid events (remove `addNotRetryableExceptions`) | **not caught** by the container tests (the event still reaches the dead-letter topic, a second later); `KafkaConfigTest` was added and now catches it |

## 9. CI

GitHub's Ubuntu runner has Docker, so `mvnw verify` runs unchanged. A small shell step turns the Surefire XML into a
table in the job summary; the reports are uploaded when a test fails.

## 10. What a real setup adds

- Contract tests or a schema registry between event producers and consumers.
- A transactional outbox and a test for it (kill the app between commit and send, check the event still goes out).
- Consumer lag and dead-letter monitoring, and a documented way to replay dead-lettered events.
- Performance tests of the consumer (throughput, rebalancing), which these functional tests do not cover.
