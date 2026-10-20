
package com.apex.reporting.modules.reporting.application;

import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.OnCellEvent;
import com.apex.reporting.modules.reporting.domain.CustomerSummaryRepository;
import com.apex.reporting.modules.reporting.events.CustomerCreatedV1Payload;
import com.apex.reporting.modules.reporting.events.ReportingCustomerContracts;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Tag("unit")
class ProjectCustomerCreatedHandlerContractTest {

    @Test
    void shouldResolveHandlerMetadataFromContract() throws Exception {

        var repository = mock(CustomerSummaryRepository.class);

        var handler = new ProjectCustomerCreatedHandler(
                repository,
                Clock.systemUTC()
        );

        var contract = ReportingCustomerContracts.CREATED_V1;
        var method = handler.getClass().getMethod("handle", CustomerCreatedV1Payload.class);
        var annotation = method.getAnnotation(OnCellEvent.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.type()).isEqualTo(contract.eventType());
        assertThat(annotation.version()).isEqualTo(contract.eventVersion());
        assertThat(method.getParameterTypes()).containsExactly(contract.payloadType());
        assertThat(method.getReturnType()).isEqualTo(void.class);
        assertThat(EventHandler.class.isAssignableFrom(handler.getClass())).isFalse();
    }
}
