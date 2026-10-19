
package com.apex.customer.config;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Objects;

@Component
public final class ApexHttpExecutionContextFilter
        extends OncePerRequestFilter {

    private final ExecutionContextProvider contextProvider;
    private final String sourceService;
    private final String cellId;

    public ApexHttpExecutionContextFilter(
            ExecutionContextProvider contextProvider,
            @Value("${spring.application.name}") String sourceService,
            @Value("${apex.cell.id}") String cellId
    ) {
        this.contextProvider =
                Objects.requireNonNull(contextProvider);
        this.sourceService = sourceService;
        this.cellId = cellId;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain
    ) throws ServletException, IOException {

        ExecutionContext context = ExecutionContext.root(
                sourceService,
                cellId
        );

        try (var scope = contextProvider.open(context)) {
            chain.doFilter(request, response);
        }
    }
}
