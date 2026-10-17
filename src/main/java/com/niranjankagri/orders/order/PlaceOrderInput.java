package com.niranjankagri.orders.order;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** The placeOrder mutation's input. The rules are checked before anything is stored (and again by the database). */
public record PlaceOrderInput(
        @NotBlank @Size(max = 100) String customer,
        @NotBlank @Size(max = 40) String sku,
        @Min(1) @Max(10) int quantity) {}
