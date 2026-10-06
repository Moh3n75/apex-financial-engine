package com.apex.apexbootstrap.api.purchase;

import com.apex.platform.messaging.consumer.ConsumerFailureReplay;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(
        "/internal/messaging/consumer-failures"
)
public class ConsumerFailureReplayController {

    private final ConsumerFailureReplay replay;


    public ConsumerFailureReplayController(
            ConsumerFailureReplay replay
    ) {

        this.replay = replay;
    }


    @PostMapping(
            "/{failureId}/replay"
    )
    public ResponseEntity<Void> replay(
            @PathVariable long failureId
    ) {

        replay.replay(
                failureId
        );

        return ResponseEntity
                .accepted()
                .build();
    }
}