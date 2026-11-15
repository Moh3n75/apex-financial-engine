package com.apex.platform.discovery;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/** Validate explicit deployment policy without creating a registry client or selecting a provider. */
@AutoConfiguration
class DiscoveryPolicyAutoConfiguration {
    @Bean
    static BeanFactoryPostProcessor apexDiscoveryPolicyPreflight(Environment environment) {
        return beans -> Binder.get(environment).bindOrCreate("apex.discovery", DiscoveryProperties.class).validate();
    }
}
