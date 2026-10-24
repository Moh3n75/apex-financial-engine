
package com.apex.order.modules.order.domain;

public enum OrderStatus {
    CREATED,
    PENDING_INVENTORY,
    PENDING_PAYMENT,
    CONFIRMED,
    CANCELLING,
    CANCELLED
}
