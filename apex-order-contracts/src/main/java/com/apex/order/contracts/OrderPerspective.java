package com.apex.order.contracts;

import java.util.UUID;

/** Local durable fact only. Absence is NOT_FOUND, never a fabricated payment/reservation. */
public record OrderPerspective(UUID orderId, String owner, String state) {}
