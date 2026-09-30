package com.apex.credit.domain.credit.model;


import java.util.UUID;


public class CreditReservation {


    private final UUID id;


    private final long creditAccountId;


    private final long transactionId;


    private final long amount;


    private ReservationStatus status;



    private CreditReservation(
            UUID id,
            long creditAccountId,
            long transactionId,
            long amount
    ){

        this.id = id;
        this.creditAccountId = creditAccountId;
        this.transactionId = transactionId;
        this.amount = amount;

        this.status = ReservationStatus.ACTIVE;
    }



    public static CreditReservation create(
            long creditAccountId,
            long transactionId,
            long amount
    ){

        if(amount <= 0){
            throw new IllegalArgumentException(
                    "Reservation amount must be positive"
            );
        }


        return new CreditReservation(
                UUID.randomUUID(),
                creditAccountId,
                transactionId,
                amount
        );
    }



    public void consume(){

        if(status != ReservationStatus.ACTIVE){
            throw new IllegalStateException(
                    "Only active reservation can be consumed"
            );
        }


        status = ReservationStatus.CONSUMED;
    }



    public void release(){

        if(status != ReservationStatus.ACTIVE){
            throw new IllegalStateException(
                    "Only active reservation can be released"
            );
        }


        status = ReservationStatus.RELEASED;
    }



    public UUID id(){
        return id;
    }


    public long creditAccountId(){
        return creditAccountId;
    }


    public long transactionId(){
        return transactionId;
    }


    public long amount(){
        return amount;
    }


    public ReservationStatus status(){
        return status;
    }

}