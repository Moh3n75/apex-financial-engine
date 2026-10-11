package com.apex.credit.application.saga;

public interface CompensationAction {


    String name();


    void compensate();

}