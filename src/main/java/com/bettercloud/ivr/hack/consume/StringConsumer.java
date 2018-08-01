package com.bettercloud.ivr.hack.consume;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.ForkJoinPool;

@Component
public class StringConsumer implements BatchMessageListener<String, String> {
    private final Logger log = LoggerFactory.getLogger(StringConsumer.class.getCanonicalName());
    private final RestTemplate restTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String OUTBOUND_TOPIC = "ivr-project-reactor-poc-outbound-baseline";

    private final ForkJoinPool forkJoinPool = new ForkJoinPool(1000);
    @Autowired
    public StringConsumer(RestTemplate restTemplate, KafkaTemplate<String, String> kafkaTemplate) {
        this.restTemplate = restTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> records) {
        log.info("Polled records: {}", records.size());
        forkJoinPool.submit(() ->
        records.parallelStream().forEach(data -> {
            final String reversed = reverseString(data.value());
            kafkaTemplate.send(OUTBOUND_TOPIC, reversed)
                    .addCallback(sendResult -> log.info(reversed), e -> log.error("Error while sending", e));
        })).join();
    }

    private String reverseString(final String stringToReverse) {
        log.info("Making request");
        return restTemplate.getForObject("http://localhost:8079?value=" + stringToReverse, String.class);

    }
}
