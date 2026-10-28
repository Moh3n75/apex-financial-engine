package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.*;

final class RpcContexts {
  static ExecutionContext currentOrRoot(ExecutionContextProvider provider, String cell) {
    try {
      return provider.requireCurrent();
    } catch (IllegalStateException missing) {
      return ExecutionContext.root(cell, cell);
    }
  }
}
