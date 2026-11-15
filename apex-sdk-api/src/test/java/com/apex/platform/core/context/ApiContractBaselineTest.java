package com.apex.platform.core.context;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import com.apex.platform.grpc.api.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ApiContractBaselineTest {
    @Test void scopesRequireOwnerThreadAndLifoAndRestorePreviousContext() throws Exception {
        var provider=new ThreadLocalExecutionContextProvider();
        var first=ExecutionContext.root("service","cell");var next=ExecutionContext.root("next","cell");
        var outer=provider.open(first);var inner=provider.open(next);
        assertThatThrownBy(outer::close).isInstanceOf(IllegalStateException.class);
        try(var executor=Executors.newSingleThreadExecutor()) {
            assertThat(executor.submit(() -> catchThrowable(inner::close)).get(1,TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class);
            assertThat(executor.submit(() -> catchThrowable(provider::requireCurrent)).get(1,TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class);
        }
        inner.close();assertThat(provider.requireCurrent()).isEqualTo(first);outer.close();
        assertThatThrownBy(outer::close).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(provider::requireCurrent).isInstanceOf(IllegalStateException.class);
    }
    @Test void optionsRetainIdentityAndDefensivelyCopyTargets() {
        var original=RequestOptions.withIdempotencyKey("stable-key");
        assertThat(original.withDeadline(Duration.ofSeconds(1)).requestId()).isEqualTo(original.requestId());
        assertThat(original.withRequestId(UUID.randomUUID()).idempotencyKey()).isEqualTo("stable-key");
        assertThat(QueryOptions.defaults().deadline()).isEqualTo(Duration.ofSeconds(5));
        var targets=new ArrayList<>(List.of("b","a"));
        var options=new CollectionOptions(targets,Duration.ofSeconds(1),true);targets.clear();
        assertThat(options.expectedResponders()).containsExactly("a","b");
        assertThatThrownBy(() -> options.expectedResponders().add("c")).isInstanceOf(UnsupportedOperationException.class);
    }
}
