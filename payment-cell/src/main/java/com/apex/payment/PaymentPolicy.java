package com.apex.payment;
import java.util.UUID;
/** Simulation decision, never a PSP authorization. */
public interface PaymentPolicy { String decision(UUID orderId); }
