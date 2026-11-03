package com.apex.platform.messaging.observation;

import io.micrometer.observation.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.transaction.support.*;

/** Internal capability adapter. No dependency on the optional observation starter. */
public final class MessagingObservation {
    public static final MessagingObservation NONE=new MessagingObservation(ObservationRegistry.NOOP);
    private final ObservationRegistry registry;
    private final Attempt noop=new Attempt(null);
    public MessagingObservation(ObservationRegistry registry){this.registry=registry;}
    public Attempt start(String operation){
        if(registry.isNoop())return noop;
        try{return new Attempt(Observation.start("apex."+operation,registry));}
        catch(RuntimeException observerFailure){return noop;}
    }
    public final class Attempt {
        private final Observation observation;
        private final AtomicBoolean stopped=new AtomicBoolean();
        private Attempt(Observation observation){this.observation=observation;}
        public void finish(String outcome){
            if(observation==null||!stopped.compareAndSet(false,true))return;
            try{observation.lowCardinalityKeyValue("apex.outcome",outcome).stop();}catch(RuntimeException observerFailure){/* never changes the owning transaction */}
        }
    }
    public void point(String operation,String outcome){start(operation).finish(outcome);}
    public void afterTransaction(String operation,String committedOutcome){
        afterTransaction(operation,()->committedOutcome);
    }
    public void afterTransaction(String operation,java.util.function.Supplier<String> committedOutcome){
        if(registry.isNoop())return;
        try{
            var attempt=start(operation);
            if(TransactionSynchronizationManager.isActualTransactionActive()&&TransactionSynchronizationManager.isSynchronizationActive()){
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization(){
                    public void afterCompletion(int status){try{attempt.finish(status==STATUS_COMMITTED?committedOutcome.get():
                        status==STATUS_ROLLED_BACK?"ROLLED_BACK":"UNKNOWN");}catch(RuntimeException observerFailure){attempt.finish("UNKNOWN");}}
                });
            }else attempt.finish("UNKNOWN");
        }catch(RuntimeException observerFailure){/* local hint admission cannot fail business work */}
    }
}
