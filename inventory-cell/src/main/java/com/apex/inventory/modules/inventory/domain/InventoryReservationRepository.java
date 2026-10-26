
package com.apex.inventory.modules.inventory.domain;

public interface InventoryReservationRepository {

    ReservationResult reserve(ReservationRequest request);
}
