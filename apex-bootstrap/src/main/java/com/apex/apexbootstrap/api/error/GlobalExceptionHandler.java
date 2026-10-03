package com.apex.apexbootstrap.api.error;

import com.apex.credit.domain.exception.InsufficientCreditException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;


@RestControllerAdvice
public class GlobalExceptionHandler {


    @ExceptionHandler(
            InsufficientCreditException.class
    )
    public ResponseEntity<ApiError>
    insufficientCredit(
            InsufficientCreditException exception
    ) {

        return ResponseEntity
                .status(
                        HttpStatus.UNPROCESSABLE_CONTENT
                )
                .body(
                        new ApiError(
                                "INSUFFICIENT_CREDIT",
                                exception.getMessage(),
                                Instant.now()
                        )
                );
    }


    @ExceptionHandler(
            IllegalArgumentException.class
    )
    public ResponseEntity<ApiError>
    invalidRequest(
            IllegalArgumentException exception
    ) {

        return ResponseEntity
                .badRequest()
                .body(
                        new ApiError(
                                "INVALID_REQUEST",
                                exception.getMessage(),
                                Instant.now()
                        )
                );
    }
}