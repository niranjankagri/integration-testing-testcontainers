package com.niranjankagri.orders.order;

import jakarta.validation.Valid;
import java.util.List;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

/** The GraphQL API (schema: src/main/resources/graphql/schema.graphqls). */
@Controller
public class OrderGraphQlController {

    private final OrderRepository orders;
    private final OrderService service;

    /** Spring injects the repository (queries) and the service (placing an order). */
    public OrderGraphQlController(OrderRepository orders, OrderService service) {
        this.orders = orders;
        this.service = service;
    }

    /** Query order(id): the order, or null when there is none (not an error). */
    @QueryMapping
    public Order order(@Argument Long id) {
        return orders.findById(id).orElse(null);
    }

    /** Query orders(customer): that customer's orders, newest first. */
    @QueryMapping
    public List<Order> orders(@Argument String customer) {
        return orders.findByCustomerOrderByCreatedAtDescIdDesc(customer);
    }

    /** Mutation placeOrder: validates the input (broken rules become a BAD_REQUEST error) and places the order. */
    @MutationMapping
    public Order placeOrder(@Argument @Valid PlaceOrderInput input) {
        return service.place(input);
    }
}
