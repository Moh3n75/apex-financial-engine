package com.apex.order.modules.order.operations;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("operations")
@EnableConfigurationProperties(OrderOperationsProperties.class)
public class StalledOrderController {
    private final StalledOrderQuery query;
    private final OrderOperationsProperties properties;

    public StalledOrderController(StalledOrderQuery query, OrderOperationsProperties properties) {
        this.query = query;
        this.properties = properties;
    }

    @GetMapping("/internal/operations/stalled-orders")
    public ResponseEntity<StalledOrderQuery.Page> find(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestParam(defaultValue = "50") int limit, @RequestParam(defaultValue = "0") int offset) {
        if (!properties.authorizes(authorization)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(query.find(limit, offset));
    }
}
