package com.apex.platform.messaging.integration;
import java.util.List;
import com.apex.platform.messaging.jdbc.PublisherRouting;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
@Tag("unit")
class PublisherRoutingTest {
    @Test void legacyAndExactVersionRoutes() {
        assertThat(new PublisherRouting(false,List.of()).destination("OLD",1,"legacy")).isEqualTo("legacy");
        var routes=new PublisherRouting(true,List.of(new PublisherRouting.Route("READY",1,"payment")),List.of("READY:1"));
        assertThat(routes.destination("READY",1,"legacy")).isEqualTo("payment");
        assertThatThrownBy(()->routes.destination("READY",2,"legacy")).hasMessageContaining("Missing");
    }
    @Test void invalidDuplicateAndMissingRequiredContractsFailStartup() {
        var route=new PublisherRouting.Route("READY",1,"payment");
        assertThatThrownBy(()->new PublisherRouting(true,List.of(route,route))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->new PublisherRouting(true,List.of(route),List.of("CANCEL:1"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->new PublisherRouting(true,List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->new PublisherRouting(true,List.of(new PublisherRouting.Route("READY",1,"a,b")))).isInstanceOf(IllegalArgumentException.class);
    }
}
