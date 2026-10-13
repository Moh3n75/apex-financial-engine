package com.apex.apexbootstrap.actuator;


import com.apex.credit.application.saga.SagaMetricsService;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;


@Component
@Endpoint(id = "saga")
public class SagaMetricsEndpoint {


    private final SagaMetricsService metricsService;


    public SagaMetricsEndpoint(
            SagaMetricsService metricsService
    ){

        this.metricsService = metricsService;

    }


    @ReadOperation
    public SagaMetricsService.SagaMetrics saga(){

        return metricsService.collect();

    }

}