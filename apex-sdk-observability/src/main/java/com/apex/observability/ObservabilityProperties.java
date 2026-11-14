package com.apex.observability;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("apex.observability")
public class ObservabilityProperties {
    private boolean enabled, activeEnabled, exportEnabled;
    private int maxRecords=1024, maxBytes=1048576, maxRecordBytes=2048, maxActive=256, batchSize=64;
    private Duration activeTtl=Duration.ofSeconds(30), exportInterval=Duration.ofMillis(100),
        exportTimeout=Duration.ofSeconds(1), shutdownTimeout=Duration.ofSeconds(1);
    private String instanceId="local";
    public void validate() {
        if(maxRecords<1||maxRecords>65536||maxBytes<64||maxBytes>67108864||maxRecordBytes<32
            ||maxRecordBytes>65536||maxRecordBytes>maxBytes||maxActive<1||maxActive>16384
            ||batchSize<1||batchSize>maxRecords||instanceId==null||!instanceId.matches("[A-Za-z0-9._:-]{1,100}"))
            throw new IllegalArgumentException("Invalid observability resource bounds");
        for(var duration:new Duration[]{activeTtl,exportInterval,exportTimeout,shutdownTimeout})
            if(duration==null||duration.toMillis()<1||duration.compareTo(Duration.ofMinutes(5))>0)
                throw new IllegalArgumentException("Finite observability time bounds required");
    }
    public boolean isEnabled(){return enabled;} public void setEnabled(boolean v){enabled=v;}
    public boolean isActiveEnabled(){return activeEnabled;} public void setActiveEnabled(boolean v){activeEnabled=v;}
    public boolean isExportEnabled(){return exportEnabled;} public void setExportEnabled(boolean v){exportEnabled=v;}
    public int getMaxRecords(){return maxRecords;} public void setMaxRecords(int v){maxRecords=v;}
    public int getMaxBytes(){return maxBytes;} public void setMaxBytes(int v){maxBytes=v;}
    public int getMaxRecordBytes(){return maxRecordBytes;} public void setMaxRecordBytes(int v){maxRecordBytes=v;}
    public int getMaxActive(){return maxActive;} public void setMaxActive(int v){maxActive=v;}
    public int getBatchSize(){return batchSize;} public void setBatchSize(int v){batchSize=v;}
    public Duration getActiveTtl(){return activeTtl;} public void setActiveTtl(Duration v){activeTtl=v;}
    public Duration getExportInterval(){return exportInterval;} public void setExportInterval(Duration v){exportInterval=v;}
    public Duration getExportTimeout(){return exportTimeout;} public void setExportTimeout(Duration v){exportTimeout=v;}
    public Duration getShutdownTimeout(){return shutdownTimeout;} public void setShutdownTimeout(Duration v){shutdownTimeout=v;}
    public String getInstanceId(){return instanceId;} public void setInstanceId(String v){instanceId=v;}
}
