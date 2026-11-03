package com.apex.observability;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.config.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.*;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnProperty(prefix="apex.observability",name="enabled",havingValue="true")
@EnableConfigurationProperties(ObservabilityProperties.class)
public class ObservabilityAutoConfiguration {
    @Bean ActiveOperations activeOperations(ObservabilityProperties properties){return new ActiveOperations(properties);}
    @Bean(destroyMethod="close") @ConditionalOnMissingBean(OperationalObserver.class)
    BoundedOperationalObserver operationalObserver(ObservabilityProperties properties,ObjectProvider<MeterRegistry> meters,
        ObjectProvider<ObservationBatchExporter> exporters){
        return new BoundedOperationalObserver(properties,meters.getIfAvailable(),exporters.getIfAvailable());
    }
    @Bean OperationalObservationHandler operationalObservationHandler(OperationalObserver observer,ActiveOperations active){return new OperationalObservationHandler(observer,active);}
    @Bean MeterFilter apexOperationalMetricBoundary(){
        return new MeterFilter(){
            public MeterFilterReply accept(Meter.Id id){
                if(!id.getName().startsWith("apex."))return MeterFilterReply.NEUTRAL;
                boolean ownMetric=java.util.Set.of("apex.observability.dropped","apex.observability.queue.records",
                    "apex.observability.queue.accounted.bytes","apex.observability.admitted","apex.observability.export.failures").contains(id.getName());
                boolean name=ownMetric||java.util.Arrays.stream(OperationalObserver.Operation.values())
                    .anyMatch(v->id.getName().equals("apex."+v.name().toLowerCase(java.util.Locale.ROOT).replace('_','.')));
                if(!name)return id.getName().startsWith("apex.observability.")?MeterFilterReply.DENY:MeterFilterReply.NEUTRAL;
                for(var tag:id.getTags()){
                    boolean safe=switch(tag.getKey()){
                        case "apex.outcome" -> java.util.Arrays.stream(OperationalObserver.Outcome.values()).anyMatch(v->v.name().equals(tag.getValue()));
                        case "reason" -> java.util.Arrays.stream(BoundedOperationalObserver.Drop.values()).anyMatch(v->v.name().equals(tag.getValue()));
                        case "error" -> tag.getValue().equals("none");
                        default -> false;
                    };
                    if(!safe)return MeterFilterReply.DENY;
                }
                return MeterFilterReply.NEUTRAL;
            }
        };
    }
}
