
package com.apex.platform.webmvc;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.core.context.ExecutionContextProvider;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.filter.OncePerRequestFilter;


@AutoConfiguration(after = ApexExecutionContextAutoConfiguration.class)
@ConditionalOnWebApplication(
        type = ConditionalOnWebApplication.Type.SERVLET
)
@ConditionalOnClass({
        OncePerRequestFilter.class,
        HttpServletRequest.class
})
@ConditionalOnProperty(
        prefix = "apex.webmvc.context",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class ApexWebMvcAutoConfiguration {

    @Bean(name = "apexHttpExecutionContextFilter")
    @ConditionalOnMissingBean(
            name = "apexHttpExecutionContextFilter"
    )
    @ConditionalOnBean(ExecutionContextProvider.class)
    ApexHttpExecutionContextFilter apexHttpExecutionContextFilter(
            ExecutionContextProvider contextProvider,
            @Value("${spring.application.name}") String sourceService,
            @Value("${apex.cell.id}") String cellId
    ) {
        return new ApexHttpExecutionContextFilter(
                contextProvider,
                sourceService,
                cellId
        );
    }
}
