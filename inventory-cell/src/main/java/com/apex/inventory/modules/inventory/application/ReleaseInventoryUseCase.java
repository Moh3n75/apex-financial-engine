package com.apex.inventory.modules.inventory.application;

import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import com.apex.inventory.modules.inventory.domain.ReservationItem;

/** Reservation row first, then the same Java UUID stock lock order used by reserve. */
@Service
public class ReleaseInventoryUseCase {
    private final JdbcTemplate jdbc;
    public ReleaseInventoryUseCase(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    @Transactional
    public boolean execute(UUID orderId) {
        String status=jdbc.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=? FOR UPDATE",String.class,orderId);
        if ("RELEASED".equals(status)) { return false; }
        if (!"RESERVED".equals(status)) { throw new IllegalStateException("Reservation cannot be released"); }
        var items=jdbc.query("SELECT product_id,quantity FROM inventory_reservation_items WHERE order_id=?",
                (rs,n)->new ReservationItem(rs.getObject(1,UUID.class),rs.getInt(2)),orderId).stream()
                .sorted(Comparator.comparing(ReservationItem::productId)).toList();
        if(items.isEmpty()) { throw new IllegalStateException("Reservation has no items"); }
        for(var item:items) {
            jdbc.queryForObject("SELECT reserved_quantity FROM inventory_stock WHERE product_id=? FOR UPDATE",Integer.class,item.productId());
        }
        for(var item:items) {
            int changed=jdbc.update("UPDATE inventory_stock SET available_quantity=available_quantity+?,reserved_quantity=reserved_quantity-? WHERE product_id=? AND reserved_quantity>=?",
                    item.quantity(),item.quantity(),item.productId(),item.quantity());
            if(changed!=1) { throw new IllegalStateException("Inconsistent reserved stock"); }
        }
        if(jdbc.update("UPDATE inventory_reservations SET status='RELEASED' WHERE order_id=? AND status='RESERVED'",orderId)!=1) {
            throw new IllegalStateException("Reservation release transition failed");
        }
        return true;
    }
}
