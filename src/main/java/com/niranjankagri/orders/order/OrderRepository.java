package com.niranjankagri.orders.order;

import java.util.List;
import org.springframework.data.repository.ListCrudRepository;

public interface OrderRepository extends ListCrudRepository<Order, Long> {

    List<Order> findByCustomerOrderByCreatedAtDescIdDesc(String customer);
}
