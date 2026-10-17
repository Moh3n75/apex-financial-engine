package com.apex.credit.application.saga;


public interface SagaMetricsService {


    SagaMetrics collect();


    record SagaMetrics(

            long total,

            long running,

            long failed,

            long compensating,

            long timedOut

    ){}

}