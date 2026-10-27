package com.apex.payment;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuthorizePaymentUseCase {
    public record Result(String status,boolean first) { }
    private final JdbcTemplate jdbc;
    private final PaymentPolicy policy;
    public AuthorizePaymentUseCase(JdbcTemplate jdbc,PaymentPolicy policy) { this.jdbc=jdbc; this.policy=policy; }
    @Transactional
    public Result execute(UUID orderId) {
        int claimed=jdbc.update("INSERT INTO payment_authorizations(order_id,status) VALUES (?,'PENDING') ON CONFLICT(order_id) DO NOTHING",orderId);
        if(claimed==0) {
            String status=jdbc.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?",String.class,orderId);
            if(!"AUTHORIZED".equals(status) && !"DECLINED".equals(status)) { throw new IllegalStateException("Nonterminal payment decision"); }
            return new Result(status,false);
        }
        String decision=policy.decision(orderId);
        if(!"AUTHORIZED".equals(decision) && !"DECLINED".equals(decision)) { throw new IllegalStateException("Invalid simulation policy"); }
        if(jdbc.update("UPDATE payment_authorizations SET status=? WHERE order_id=? AND status='PENDING'",decision,orderId)!=1) {
            throw new IllegalStateException("Payment claim lost");
        }
        return new Result(decision,true);
    }
}
