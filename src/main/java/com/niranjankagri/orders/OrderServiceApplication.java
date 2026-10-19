package com.niranjankagri.orders;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/** The order service: the system under test of this repository. */
@SpringBootApplication
public class OrderServiceApplication {

    /**
     * Starts the service; it needs PostgreSQL and Kafka (see TestOrderServiceApplication for a local run with
     * containers).
     */
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    /** The time source, as a bean so a test could replace it with a fixed clock. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
