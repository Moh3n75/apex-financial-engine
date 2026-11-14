package com.apex.platform.grpc.internal;

import io.micrometer.observation.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Logical attempts only. No transport retry, payload, IDs, or business commit claim. */
final class RpcObservation {
    static final RpcObservation NONE=new RpcObservation(ObservationRegistry.NOOP);
    private final ObservationRegistry registry;
    private final Attempt noop=new Attempt(null);
    RpcObservation(ObservationRegistry registry){this.registry=registry;}
    Attempt start(String operation){
        if(registry.isNoop())return noop;
        try{return new Attempt(Observation.start("apex."+operation,registry));}
        catch(RuntimeException observerFailure){return noop;}
    }
    final class Attempt {
        private final Observation observation;
        private final AtomicBoolean stopped=new AtomicBoolean();
        private Attempt(Observation observation){this.observation=observation;}
        void finish(String outcome){if(observation!=null&&stopped.compareAndSet(false,true))try{
            observation.lowCardinalityKeyValue("apex.outcome",outcome).stop();
        }catch(RuntimeException observerFailure){/* never propagates into gateway/deadline completion */}}
    }
    static String outcome(Throwable failure){
        if(failure==null)return "SUCCEEDED";
        return switch(RpcErrors.failure(failure,false).code()){
            case DEADLINE_EXCEEDED -> "DEADLINE_EXCEEDED";
            case CANCELLED -> "CANCELLED";
            case UNAVAILABLE -> "UNKNOWN";
            default -> "FAILED";
        };
    }
    <R> CompletionStage<R> watch(String operation,Supplier<CompletionStage<R>> call){
        if(registry.isNoop())return call.get();
        var attempt=start(operation);
        try{
            var original=call.get();original.whenComplete((value,failure)->attempt.finish(outcome(failure)));
            return original; // Preserve exact cancellation/exception/partial-result semantics.
        }catch(RuntimeException failure){attempt.finish(outcome(failure));throw failure;}
    }
}
