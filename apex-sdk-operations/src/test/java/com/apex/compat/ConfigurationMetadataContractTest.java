package com.apex.compat;

import java.beans.Introspector;
import java.time.Duration;
import java.util.*;
import com.apex.platform.grpc.internal.*;
import com.apex.platform.messaging.jdbc.*;
import com.apex.platform.discovery.DiscoveryProperties;
import com.apex.observability.ObservabilityProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.mock.env.MockEnvironment;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

class ConfigurationMetadataContractTest {
    @Test void generatedTypedDefaultsMatchActualBoundDefaults() throws Exception {
        var types=Map.of("apex.grpc",GrpcProperties.class,"apex.grpc.collector",CollectorProperties.class,
                "apex.grpc.security",PeerSecurityProperties.class,"apex.discovery",DiscoveryProperties.class,
                "apex.observability",ObservabilityProperties.class,"apex.messaging.provisioning",TopicProvisioningProperties.class,
                "apex.messaging.publisher.routing",PublisherRouting.class,"apex.messaging.consumer.reliability",ConsumerReliabilityProperties.class);
        var mapper=JsonMapper.builder().build();
        var resources=getClass().getClassLoader().getResources("META-INF/spring-configuration-metadata.json");
        int checked=0;
        while(resources.hasMoreElements()) {
            try(var stream=resources.nextElement().openStream()) {
                var properties=mapper.readTree(stream).get("properties"); if(properties==null)continue;
                for(var property:properties) {
                    String name=property.get("name").asText(); var expected=property.get("defaultValue");
                    if(expected==null || property.get("sourceType")==null || !name.startsWith("apex."))continue;
                    String prefix=types.keySet().stream().filter(k->name.startsWith(k+".")).max(Comparator.comparingInt(String::length)).orElse(null);
                    if(prefix==null)continue;
                    String tail=name.substring(prefix.length()+1);Object value=Binder.get(new MockEnvironment()).bindOrCreate(prefix,types.get(prefix));
                    for(String part:tail.split("\\.")) {
                        String member=kebabToCamel(part);
                        if(value.getClass().isRecord())value=value.getClass().getMethod(member).invoke(value);
                        else {
                            var descriptor=Arrays.stream(Introspector.getBeanInfo(value.getClass()).getPropertyDescriptors())
                                    .filter(p->p.getName().equals(member)).findFirst().orElseThrow();
                            value=descriptor.getReadMethod().invoke(value);
                        }
                    }
                    if(value instanceof Duration duration)assertThat(duration).as(name).isEqualTo(org.springframework.boot.convert.DurationStyle.detectAndParse(expected.asText()));
                    else if(value instanceof Number number)assertThat(number.longValue()).as(name).isEqualTo(expected.longValue());
                    else assertThat((tools.jackson.databind.JsonNode)mapper.valueToTree(value)).as(name).isEqualTo(expected);
                    checked++;
                }
            }
        }
        assertThat(checked).isGreaterThan(35);
    }
    private String kebabToCamel(String text) {
        var result=new StringBuilder();boolean upper=false;
        for(char c:text.toCharArray())if(c=='-')upper=true;else {result.append(upper?Character.toUpperCase(c):c);upper=false;}
        return result.toString();
    }
}
