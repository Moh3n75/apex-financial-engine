
package com.apex.platform.context.autoconfigure;

import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class ApexExecutionContextAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ExecutionContextProvider.class)
    public ExecutionContextProvider executionContextProvider() {
        return new ThreadLocalExecutionContextProvider();
    }
}
