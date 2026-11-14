package com.apex.observability;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongSupplier;

/** Process-local observation only. Expiry is lost visibility, never completion or commit. */
public final class ActiveOperations {
    public record Entry(String instanceId, UUID bootEpoch, UUID attemptId,
        OperationalObserver.Operation operation, Instant observedStart, long elapsedNanos) {}
    private record Running(OperationalObserver.Operation operation, Instant start, long nanos) {}
    private final ConcurrentHashMap<UUID,Running> active=new ConcurrentHashMap<>();
    private final Semaphore slots;
    private final UUID epoch=UUID.randomUUID();
    private final ObservabilityProperties policy;
    private final LongSupplier nanos;
    private final LongAdder expired=new LongAdder(), rejected=new LongAdder();
    public ActiveOperations(ObservabilityProperties policy){this(policy,System::nanoTime);}
    ActiveOperations(ObservabilityProperties policy,LongSupplier nanos){policy.validate();this.policy=policy;this.nanos=nanos;slots=new Semaphore(policy.getMaxActive());}
    public Handle tryRegister(OperationalObserver.Operation operation){
        if(!policy.isActiveEnabled()||operation==null)return new Handle(null);
        expire();
        if(!slots.tryAcquire()){rejected.increment();return new Handle(null);}
        UUID id=UUID.randomUUID();active.put(id,new Running(operation,Instant.now(),nanos.getAsLong()));
        return new Handle(id);
    }
    public final class Handle implements AutoCloseable {
        private final UUID id;
        private Handle(UUID id){this.id=id;}
        public void close(){if(id!=null&&active.remove(id)!=null)slots.release();}
    }
    private void expire(){long now=nanos.getAsLong();active.forEach((id,v)->{
        if(now-v.nanos()>=policy.getActiveTtl().toNanos()&&active.remove(id,v)){slots.release();expired.increment();}
    });}
    public List<Entry> snapshot(){expire();long now=nanos.getAsLong();return active.entrySet().stream().map(e->new Entry(
        policy.getInstanceId(),epoch,e.getKey(),e.getValue().operation(),e.getValue().start(),Math.max(0,now-e.getValue().nanos())))
        .sorted(Comparator.comparing(Entry::attemptId)).toList();}
    public long expiredCount(){return expired.sum();} public long rejectedCount(){return rejected.sum();}
    public int size(){expire();return active.size();}
}
