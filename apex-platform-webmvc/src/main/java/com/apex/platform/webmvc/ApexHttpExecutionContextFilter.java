
package com.apex.platform.webmvc;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Objects;

public final class ApexHttpExecutionContextFilter
        extends OncePerRequestFilter {

    private final ExecutionContextProvider contextProvider;
    private final String sourceService;
    private final String cellId;

    public ApexHttpExecutionContextFilter(
            ExecutionContextProvider contextProvider,
            String sourceService,
            String cellId
    ) {
        this.contextProvider =
                Objects.requireNonNull(contextProvider);

        this.sourceService =
                requireText(sourceService, "sourceService");

        this.cellId =
                requireText(cellId, "cellId");
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

    private static String requireText(
            String value,
            String name
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    name + " must not be blank"
            );
        }

        return value;
    }
}
