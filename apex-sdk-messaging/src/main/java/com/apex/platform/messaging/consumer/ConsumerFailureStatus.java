package com.apex.platform.messaging.consumer;

public enum ConsumerFailureStatus {

    RETRYING,

    DEAD_LETTERED,

    RECOVERY_FAILED,

    REPLAYING,

    REPLAY_FAILED,

    RECOVERED
}