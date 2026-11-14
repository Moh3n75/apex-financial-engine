
package com.apex.platform.core.context;

public interface ExecutionContextProvider {

    ExecutionContext requireCurrent();

    Scope open(ExecutionContext context);

    interface Scope extends AutoCloseable {

        @Override
        void close();
    }
}
