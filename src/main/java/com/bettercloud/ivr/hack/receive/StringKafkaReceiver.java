package com.bettercloud.ivr.hack.receive;

import com.bettercloud.ivr.hack.query.WebReverseService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * todo javadoc.
 * I'm a loser
 */
@Component
public class StringKafkaReceiver {
    private static final Logger log = LoggerFactory.getLogger(StringKafkaReceiver.class.getCanonicalName());
    private final WebReverseService webReverseService;
    private final Flux flux;

    private static final String TOPIC = "ivr-project-reactor-poc-topic2";

    @Autowired
    public StringKafkaReceiver(WebReverseService webReverseService) {
        this.webReverseService = webReverseService;

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ivr-project-reactor-poc-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(TOPIC));

        Scheduler scheduler = Schedulers.newElastic("sample", 60, true); // todo look into this
        flux = KafkaReceiver.create(receiverOptions)
                .receive() // todo on close or something?
                .groupBy(message -> message.receiverOffset().topicPartition())
                .flatMap(patitionFlux ->
                        patitionFlux.publishOn(scheduler)
                                .flatMapSequential(receivedRecord -> process(receivedRecord), 64, 1)
                                .sample(Duration.ofMillis(2000))
                                .concatMap(offset -> {
                                    log.info("COMMIT: " + offset.offset());
                                    return offset.commit();
                                }));
    }

    private Mono<ReceiverOffset> process(ReceiverRecord<String, String> record) {
        return webReverseService.reverseString(record)
                .thenReturn(record.receiverOffset());
    }

    @PostConstruct
    public void init() {
        flux.subscribe();
    }
}
