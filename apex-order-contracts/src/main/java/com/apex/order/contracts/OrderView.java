package com.apex.order.contracts;

import java.util.UUID;

public record OrderView(UUID orderId, String status) {}
