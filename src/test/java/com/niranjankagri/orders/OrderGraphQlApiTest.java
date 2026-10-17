package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.execution.ErrorType;
import org.springframework.graphql.test.tester.HttpGraphQlTester;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The GraphQL API over real HTTP, backed by the real database. GraphQL answers HTTP 200 even for errors, so every
 * test looks at the "errors" array and the data, never only at the status code.
 */
@IntegrationTest
class OrderGraphQlApiTest {

    private static final String PLACE = """
            mutation($customer: String!, $sku: String!, $quantity: Int!) {
              placeOrder(input: {customer: $customer, sku: $sku, quantity: $quantity}) {
                id customer sku quantity status createdAt
              }
            }""";

    @Autowired
    HttpGraphQlTester graphQl;

    @Autowired
    JdbcClient jdbc;

    /** Each test uses its own customer, so tests never see each other's data, in any order or in parallel. */
    private final String customer = "customer-" + UUID.randomUUID();

    @Test
    void placeOrderStoresTheOrderAsPlaced() {
        String id = place(customer, "SKU-COFFEE", 2);

        graphQl.document("query($id: ID!) { order(id: $id) { id customer sku quantity status } }")
                .variable("id", id)
                .execute()
                .errors().verify()
                .path("order.customer").entity(String.class).isEqualTo(customer)
                .path("order.sku").entity(String.class).isEqualTo("SKU-COFFEE")
                .path("order.quantity").entity(Integer.class).isEqualTo(2)
                .path("order.status").entity(String.class).isEqualTo("PLACED");
    }

    @Test
    void ordersListsOnlyTheCustomersOwnOrdersNewestFirst() {
        String first = place(customer, "SKU-A", 1);
        String second = place(customer, "SKU-B", 1);
        place("someone-else-" + UUID.randomUUID(), "SKU-C", 1);

        List<String> ids = graphQl.document("query($c: String!) { orders(customer: $c) { id } }")
                .variable("c", customer)
                .execute()
                .errors().verify()
                .path("orders[*].id").entityList(String.class).get();
        assertThat(ids).containsExactly(second, first);
    }

    @Test
    void unknownOrderIsNullWithoutAnError() {
        graphQl.document("{ order(id: \"999999999\") { id } }")
                .execute()
                .errors().verify()
                .path("order").valueIsNull();
    }

    @Test
    void invalidInputIsABadRequestErrorAndNothingIsStored() {
        graphQl.document(PLACE)
                .variable("customer", " ")
                .variable("sku", "SKU-A")
                .variable("quantity", 11)
                .execute()
                .errors()
                .expect(error -> error.getErrorType() == ErrorType.BAD_REQUEST
                        && error.getMessage().equals("customer: must not be blank; quantity: must be less than or equal to 10"))
                .verify()
                // placeOrder returns Order! (non-null). When it fails, GraphQL cannot return null for it, so the null
                // propagates to the parent: the whole "data" is null, not just this field.
                .path("placeOrder").pathDoesNotExist();

        Integer stored = jdbc.sql("SELECT count(*) FROM orders WHERE sku = 'SKU-A' AND quantity = 11")
                .query(Integer.class)
                .single();
        assertThat(stored).isZero();
    }

    @Test
    void aMissingRequiredFieldIsRejectedByTheSchemaBeforeAnyCodeRuns() {
        graphQl.document("mutation { placeOrder(input: {customer: \"x\", sku: \"y\"}) { id } }")
                .execute()
                .errors()
                .expect(error -> error.getMessage().contains("quantity"))
                .verify();
    }

    @Test
    void anUnknownFieldIsRejectedByTheSchema() {
        graphQl.document("{ order(id: \"1\") { id price } }")
                .execute()
                .errors()
                .expect(error -> error.getMessage().contains("price"))
                .verify();
    }

    private String place(String forCustomer, String sku, int quantity) {
        return graphQl.document(PLACE)
                .variable("customer", forCustomer)
                .variable("sku", sku)
                .variable("quantity", quantity)
                .execute()
                .errors().verify()
                .path("placeOrder.status").entity(String.class).isEqualTo("PLACED")
                .path("placeOrder.id").entity(String.class).get();
    }
}
