
package com.apex.platform.core.context;

import java.util.Objects;

public final class ThreadLocalExecutionContextProvider
        implements ExecutionContextProvider {

    private final ThreadLocal<ActiveScope> currentScope =
            new ThreadLocal<>();

    @Override
    public ExecutionContext requireCurrent() {
        ActiveScope scope = currentScope.get();

        if (scope == null) {
            throw new IllegalStateException(
                    "No active APEX execution context"
            );
        }

        return scope.context;
    }

    @Override
    public Scope open(ExecutionContext context) {
        Objects.requireNonNull(context, "context");

        ActiveScope scope = new ActiveScope(
                context,
                currentScope.get(),
                Thread.currentThread()
        );

        currentScope.set(scope);
        return scope;
    }

    private final class ActiveScope implements Scope {

        private final ExecutionContext context;
        private final ActiveScope previous;
        private final Thread owner;
        private boolean closed;

        private ActiveScope(
                ExecutionContext context,
                ActiveScope previous,
                Thread owner
        ) {
            this.context = context;
            this.previous = previous;
            this.owner = owner;
        }

        @Override
        public void close() {
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException(
                        "Execution context scope closed on another thread"
                );
            }

            if (closed || currentScope.get() != this) {
                throw new IllegalStateException(
                        "Execution context scopes must close in order"
                );
            }

            closed = true;

            if (previous == null) {
                currentScope.remove();
            } else {
                currentScope.set(previous);
            }
        }
    }
}
