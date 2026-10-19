package com.niranjankagri.orders.order;

import java.util.List;
import org.springframework.data.repository.ListCrudRepository;

/** Spring Data JDBC repository for the orders table; Spring writes the query from the method name. */
public interface OrderRepository extends ListCrudRepository<Order, Long> {

    List<Order> findByCustomerOrderByCreatedAtDescIdDesc(String customer);
}
