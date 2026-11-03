package com.apex.observability;

import io.micrometer.observation.*;
import java.util.*;

/** Only fixed APEX operations and outcome enums cross this boundary; no Throwable/tags copied. */
public final class OperationalObservationHandler implements ObservationHandler<Observation.Context> {
    private static final String START="apex.ops.start", HANDLE="apex.ops.active";
    private static final Map<String,OperationalObserver.Operation> NAMES;
    static{
        var names=new HashMap<String,OperationalObserver.Operation>();
        for(var operation:OperationalObserver.Operation.values())names.put("apex."+operation.name().toLowerCase(Locale.ROOT).replace('_','.'),operation);
        NAMES=Map.copyOf(names);
    }
    private final OperationalObserver observer;
    private final ActiveOperations active;
    public OperationalObservationHandler(OperationalObserver observer,ActiveOperations active){this.observer=observer;this.active=active;}
    public boolean supportsContext(Observation.Context context){
        // Native documentation-based observations select handlers before naming the context.
        return context!=null&&context.getName()!=null&&NAMES.containsKey(context.getName());
    }
    public void onStart(Observation.Context context){
        try{context.put(START,System.nanoTime());context.put(HANDLE,active.tryRegister(NAMES.get(context.getName())));}catch(RuntimeException ignored){/* diagnostics never own the operation */}
    }
    public void onStop(Observation.Context context){
        try{
            OperationalObserver.Outcome outcome=OperationalObserver.Outcome.OBSERVED;
            var value=context.getLowCardinalityKeyValue("apex.outcome");
            if(value!=null)try{outcome=OperationalObserver.Outcome.valueOf(value.getValue());}catch(IllegalArgumentException ignored){}
            Long start=context.get(START);
            observer.tryRecord(new OperationalObserver.Sample(NAMES.get(context.getName()),outcome,
                start==null?0:Math.max(0,System.nanoTime()-start)));
        }catch(RuntimeException ignored){/* never log a context or payload */}
        finally{ActiveOperations.Handle handle=context.get(HANDLE);if(handle!=null)handle.close();}
    }
}
