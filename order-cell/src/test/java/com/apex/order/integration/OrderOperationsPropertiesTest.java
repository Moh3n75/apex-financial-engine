package com.apex.order.integration;

import static org.assertj.core.api.Assertions.*;
import com.apex.order.modules.order.operations.OrderEvidenceHandler;
import com.apex.operations.contracts.*;
import com.apex.platform.grpc.api.CellQueryHandler;
import java.util.*;
import org.junit.jupiter.api.Test;

/** The legacy bearer-token admin surface was intentionally removed; owner reads use mTLS contracts. */
class OrderOperationsPropertiesTest {
    @Test void retiredTokenAdminSurfaceIsNotReintroduced() {
        for (String type : List.of("OrderOperationsProperties","StalledOrderController","StalledOrderQuery")) {
            assertThatThrownBy(() -> Class.forName("com.apex.order.modules.order.operations." + type))
                    .isInstanceOf(ClassNotFoundException.class);
        }
    }
    @Test void replacementOwnerReadUsesTheVersionedQueryContract() throws Exception {
        var handler = OrderEvidenceHandler.class.getMethod("read", EvidenceQuery.class).getAnnotation(CellQueryHandler.class);
        assertThat(handler.type()).isEqualTo("OPS_OWNER_SNAPSHOT");
        assertThat(handler.version()).isEqualTo(1);
        UUID id=UUID.randomUUID();
        assertThatThrownBy(() -> new EvidenceQuery(List.of(id,id),List.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
