package com.bettercloud.ivr.hack.query;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


/**
 * Todo can probably optiumize WebClient creation here
 * Todo wire up to actual service
 */
@Component
public class WebReverseServiceImpl implements WebReverseService {
    private static final Logger log = LoggerFactory.getLogger(WebReverseServiceImpl.class.getCanonicalName());

    private final WebClient webClient = WebClient.create("http://localhost:8080");

    private static final String OUTBOUND_TOPIC = "ivr-project-reactor-poc-qq";

    private KafkaSender<String, String> kafkaSender;

    public WebReverseServiceImpl() {
        Map<String, Object> senderProps = new HashMap<>();
        senderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        SenderOptions senderOptions = SenderOptions.create(senderProps)
                .maxInFlight(1024);

        this.kafkaSender = KafkaSender.create(senderOptions);
    }

    @Override
    public Mono<Void> reverseString(ReceiverRecord<String, String> stringToReverse) {
        Random random = new Random();
        int secondsToDelay = Math.abs(random.nextInt() % 8);

        return kafkaSender.send(Mono.just(stringToReverse)
                .delayElement(Duration.ofSeconds(secondsToDelay))
                .map(string -> {
                    final StringBuilder stringBuilder = new StringBuilder(string.value());
                    log.info("PROCESSED: " + string.offset() + ", DELAY: " + secondsToDelay);
                    final String reversedString = stringBuilder.reverse().toString();
                    return SenderRecord.create(new ProducerRecord<>(OUTBOUND_TOPIC, "tempKey", reversedString), 1);
                }))
                .then();

        //        return webClient.get()
//                .uri("/" + stringToReverse)
//                .accept(MediaType.TEXT_PLAIN)
//                .exchange()
//                .flatMap(clientResponse -> clientResponse.bodyToMono(String.class))
//                .block();
    }
}
