
package com.apex.inventory.modules.inventory.application;

import com.apex.inventory.modules.inventory.domain.*;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ReserveInventoryUseCase {

    private final InventoryReservationRepository repository;

    public ReserveInventoryUseCase(
            InventoryReservationRepository repository
    ) {
        this.repository = repository;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ReservationResult execute(ReservationRequest request) {
        return repository.reserve(request);
    }
}
