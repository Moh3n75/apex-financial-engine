package com.apex.apexbootstrap.api.purchase;

import com.apex.credit.application.purchase.PurchaseApplicationService;
import com.apex.credit.application.purchase.PurchaseCommand;
import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.CreditAmount;

import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/purchases")
public class PurchaseController {

    private final PurchaseApplicationService purchaseService;

    public PurchaseController(
            PurchaseApplicationService purchaseService
    ) {
        this.purchaseService =
                purchaseService;
    }


    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PurchaseResponse purchase(
            @Valid
            @RequestBody
            PurchaseRequest request
    ) {

        var result =
                purchaseService.execute(

                        new PurchaseCommand(

                                new CreditAccountId(
                                        request.sourceAccountId()
                                ),

                                new CreditAccountId(
                                        request.destinationAccountId()
                                ),

                                new CreditAmount(
                                        request.amountUnits()
                                ),

                                request.referenceId()
                        )
                );


        return new PurchaseResponse(

                result.transactionId()
                        .value(),

                result.status()
                        .name()
        );
    }
}