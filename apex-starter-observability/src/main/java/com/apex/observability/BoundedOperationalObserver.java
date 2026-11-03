package com.apex.observability;

import io.micrometer.core.instrument.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;

/** Drop-new diagnostics, never durable evidence. Fixed reservation bounds metadata storage.
 * No caller waits for a lock, exporter, network, queue capacity or exporter shutdown. */
public final class BoundedOperationalObserver implements OperationalObserver, AutoCloseable {
    public enum Drop { FULL, CONTENDED, OVERSIZE, CLOSED, EXPORT_FAILURE, EXPORT_TIMEOUT }
    public record Statistics(int queuedRecords,int accountedBytes,int maxRecordsObserved,int maxBytesObserved,
        long admitted,long dropped,long exportFailures,boolean circuitOpen) {}
    public static final int RECORD_RESERVATION_BYTES=256;
    private final ObservabilityProperties policy;
    private final ReentrantLock lock=new ReentrantLock();
    private final ArrayDeque<Sample> queue=new ArrayDeque<>();
    private final EnumMap<Drop,LongAdder> drops=new EnumMap<>(Drop.class);
    private final LongAdder admitted=new LongAdder(), exportFailures=new LongAdder();
    private final AtomicInteger highRecords=new AtomicInteger(), highBytes=new AtomicInteger();
    private final AtomicBoolean closed=new AtomicBoolean();
    private volatile boolean circuitOpen;
    private final ScheduledExecutorService pump;
    private final ExecutorService sender;
    public BoundedOperationalObserver(ObservabilityProperties policy,MeterRegistry meters,ObservationBatchExporter exporter){
        policy.validate();this.policy=policy;
        for(var reason:Drop.values()){
            var count=new LongAdder();drops.put(reason,count);
            if(meters!=null)FunctionCounter.builder("apex.observability.dropped",count,LongAdder::doubleValue)
                .tag("reason",reason.name()).register(meters);
        }
        if(meters!=null){
            Gauge.builder("apex.observability.queue.records",this,x->x.statistics().queuedRecords()).register(meters);
            Gauge.builder("apex.observability.queue.accounted.bytes",this,x->x.statistics().accountedBytes()).register(meters);
            FunctionCounter.builder("apex.observability.admitted",admitted,LongAdder::doubleValue).register(meters);
            FunctionCounter.builder("apex.observability.export.failures",exportFailures,LongAdder::doubleValue).register(meters);
        }
        if(policy.isExportEnabled()&&exporter!=null){
            sender=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(1),
                Thread.ofPlatform().daemon().name("apex-observation-send-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
            pump=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("apex-observation-pump-",0).factory());
            pump.scheduleWithFixedDelay(()->export(exporter),policy.getExportInterval().toMillis(),
                policy.getExportInterval().toMillis(),TimeUnit.MILLISECONDS);
        }else{sender=null;pump=null;}
    }
    @Override public boolean tryRecord(Sample sample){
        try{
            if(sample==null)return false;
            if(closed.get())return drop(Drop.CLOSED);
            if(circuitOpen)return drop(Drop.EXPORT_TIMEOUT);
            if(RECORD_RESERVATION_BYTES>policy.getMaxRecordBytes())return drop(Drop.OVERSIZE);
            if(!lock.tryLock())return drop(Drop.CONTENDED);
            try{
                if(closed.get())return drop(Drop.CLOSED);
                if(queue.size()>=policy.getMaxRecords()||((long)queue.size()+1)*RECORD_RESERVATION_BYTES>policy.getMaxBytes())return drop(Drop.FULL);
                queue.addLast(sample);admitted.increment();
                highRecords.accumulateAndGet(queue.size(),Math::max);
                highBytes.accumulateAndGet(queue.size()*RECORD_RESERVATION_BYTES,Math::max);return true;
            }finally{lock.unlock();}
        }catch(RuntimeException diagnosticFailure){return false;}
    }
    private boolean drop(Drop reason){drops.get(reason).increment();return false;}
    public List<Sample> drain(int maximum){
        if(maximum<1||maximum>policy.getMaxRecords()||!lock.tryLock())return List.of();
        try{var batch=new ArrayList<Sample>(Math.min(maximum,queue.size()));while(batch.size()<maximum&&!queue.isEmpty())batch.add(queue.removeFirst());return List.copyOf(batch);}
        finally{lock.unlock();}
    }
    public Statistics statistics(){
        int count=-1;if(lock.tryLock()){try{count=queue.size();}finally{lock.unlock();}}
        return new Statistics(count,count<0?-1:count*RECORD_RESERVATION_BYTES,highRecords.get(),highBytes.get(),
            admitted.sum(),drops.values().stream().mapToLong(LongAdder::sum).sum(),exportFailures.sum(),circuitOpen);
    }
    public long dropped(Drop reason){return drops.get(reason).sum();}
    private void export(ObservationBatchExporter exporter){
        if(closed.get()||circuitOpen)return;
        var batch=drain(policy.getBatchSize());if(batch.isEmpty())return;
        Future<?> task=null;
        try{
            task=sender.submit(()->{try{exporter.export(batch);}catch(Exception failure){throw new CompletionException(failure);}});
            task.get(policy.getExportTimeout().toMillis(),TimeUnit.MILLISECONDS);
        }catch(TimeoutException timeout){
            circuitOpen=true;exportFailures.increment();drops.get(Drop.EXPORT_TIMEOUT).add(batch.size());
            if(task!=null)task.cancel(true); // Never create replacement threads after an uncooperative sender.
        }catch(Exception failure){
            exportFailures.increment();drops.get(Drop.EXPORT_FAILURE).add(batch.size());
            if(failure instanceof InterruptedException)Thread.currentThread().interrupt();
        }
    }
    @Override public void close(){
        if(!closed.compareAndSet(false,true))return;
        if(pump!=null)pump.shutdownNow();if(sender!=null)sender.shutdownNow();
        long end=System.nanoTime()+policy.getShutdownTimeout().toNanos();
        for(var executor:new ExecutorService[]{pump,sender})if(executor!=null)try{
            executor.awaitTermination(Math.max(0,end-System.nanoTime()),TimeUnit.NANOSECONDS);
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();}
        var remaining=drain(policy.getMaxRecords());drops.get(Drop.CLOSED).add(remaining.size());
    }
}
