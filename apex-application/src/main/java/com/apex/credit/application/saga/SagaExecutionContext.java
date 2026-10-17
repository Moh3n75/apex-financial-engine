package com.apex.credit.application.saga;


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class SagaExecutionContext {


    private final UUID sagaId;


    private final Map<String, Object> data =
            new HashMap<>();

    public Map<String,Object> data(){

        return data;

    }



    public SagaExecutionContext(
            UUID sagaId
    ){

        this.sagaId = sagaId;

    }



    public UUID sagaId(){

        return sagaId;

    }



    public void put(
            String key,
            Object value
    ){

        data.put(
                key,
                value
        );

    }



    public Object get(
            String key
    ){

        return data.get(key);

    }



    public boolean contains(
            String key
    ){

        return data.containsKey(key);

    }

}