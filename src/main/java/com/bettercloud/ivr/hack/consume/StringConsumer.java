package com.bettercloud.ivr.hack.consume;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * todo javadoc.
 */
@Component
public class StringConsumer implements MessageListener<String, String> {

    private final RestTemplate restTemplate;
    private final KafkaTemplate kafkaTemplate;
    private static final String OUTBOUND_TOPIC = "ivr-project-reactor-poc-outbound-baseline";

    @Autowired
    public StringConsumer(RestTemplate restTemplate, KafkaTemplate kafkaTemplate) {
        this.restTemplate = restTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> data) {
        kafkaTemplate.send(OUTBOUND_TOPIC, reverseString(data.value()));
    }

    private String reverseString(final String stringToReverse) {
        // todo michael
        return "placeholder";
//        final URI uri = UriComponentsBuilder.fromUriString("localhost:8080")
//                .path("/waffles")
//                .queryParam("id", stringToReverse)
//                .build()
//                .toUri();
//
//        return restTemplate.exchange(uri, HttpMethod.GET, null, String.class).getBody();
    }
}
