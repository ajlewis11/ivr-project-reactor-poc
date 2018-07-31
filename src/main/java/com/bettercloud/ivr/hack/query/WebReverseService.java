package com.bettercloud.ivr.hack.query;

import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public interface WebReverseService {
    Mono<Void> reverseString(ReceiverRecord<String, String> stringToReverse);
}
