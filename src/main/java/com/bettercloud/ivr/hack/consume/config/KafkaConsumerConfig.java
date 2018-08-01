package com.bettercloud.ivr.hack.consume.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ivr-project-reactor-poc-inboud-baseline";

    @Bean
    public ConsumerFactory consumerFactory() {

        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ivr-project-reactor-poc-baseline");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ContainerProperties containerProperties(
            final BatchMessageListener messageListener) {
        final ContainerProperties containerProperties = new ContainerProperties(TOPIC);
        containerProperties.setMessageListener(messageListener);
        return containerProperties;
    }

    @Bean
    public <T> KafkaMessageListenerContainer<String, T> kafkaMessageListenerContainer(
            final ConsumerFactory<String, T> consumerFactory,
            final ContainerProperties containerProperties) {
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }
}
