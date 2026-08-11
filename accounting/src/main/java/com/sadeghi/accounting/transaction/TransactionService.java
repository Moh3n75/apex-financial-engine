package com.sadeghi.accounting.transaction;

import com.sadeghi.accounting.transaction.controller.TransactionModel;
import com.sadeghi.accounting.transaction.orm.AccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class TransactionService {

    @Autowired
    private AccountRepository accountRepository;

    private final Counter transactionSuccessCounter;
    private final Counter transactionFailureCounter;
    private final Timer transactionTimer;


    public TransactionService(MeterRegistry meterRegistry) {

        this.transactionSuccessCounter = Counter.builder("transactions")
                .tag("status", "success")
                .description("Successfully processed transactions")
                .register(meterRegistry);

        this.transactionFailureCounter = Counter.builder("transactions")
                .tag("status", "failure")
                .description("Failed transactions")
                .register(meterRegistry);

        this.transactionTimer = Timer.builder("transaction_processing")
                .description("Transaction processing latency")
                .publishPercentiles(0.50, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }



    @Transactional(rollbackFor = Exception.class)
    public TransactionModel create(TransactionModel transactionModel) {

        /*
        First code without Lock / JPA Read-Modify-Write
        Account source = accountRepository.findById(transactionModel.sourceAccount()).orElseThrow();
        Account destination = accountRepository.findById(transactionModel.destinationAccount()).orElseThrow();*/

        //**We don't use the Optimistic lock because we need to implement the retry model


        /*
        Second use the Pessimistic lock
        Account source = accountRepository.findByIdForUpdate(transactionModel.sourceAccount()).orElseThrow();
        Account destination = accountRepository.findByIdForUpdate(transactionModel.destinationAccount()).orElseThrow();

        source.setAvailableAmount(source.getAvailableAmount().add(transactionModel.amount().negate()));
        source.setTotalAmount(source.getTotalAmount().add(transactionModel.amount().negate()));

        accountRepository.save(source);

        destination.setAvailableAmount(destination.getAvailableAmount().add(transactionModel.amount()));
        destination.setTotalAmount(destination.getTotalAmount().add(transactionModel.amount()));

        accountRepository.save(destination);*/


        //Third use the Query native
        return transactionTimer.record(() -> {

            try {

                int decreased = accountRepository.decreaseBalance(
                        transactionModel.sourceAccount(),
                        transactionModel.amount()
                );

                if (decreased != 1) {
                    throw new Exception(
                            "Insufficient balance or source account not found"
                    );
                }

                int increased = accountRepository.increaseBalance(
                        transactionModel.destinationAccount(),
                        transactionModel.amount()
                );

                if (increased != 1) {
                    throw new Exception(
                            "Destination account not found"
                    );
                }

                transactionSuccessCounter.increment();

                return transactionModel;

            } catch (Exception e) {

                transactionFailureCounter.increment();

                try {
                    throw e;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
    }
}
