package com.apex.payment;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.jdbc.core.JdbcTemplate;
@Component
@Profile("simulation")
public class SeededSimulationPolicy implements PaymentPolicy {
    private final JdbcTemplate jdbc;
    public SeededSimulationPolicy(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    public String decision(UUID orderId) {
        var decisions=jdbc.query("SELECT decision FROM payment_simulation_policy WHERE order_id=?",(rs,n)->rs.getString(1),orderId);
        if(decisions.isEmpty()) { throw new IllegalStateException("Explicit simulation policy missing"); }
        return decisions.getFirst();
    }
}
