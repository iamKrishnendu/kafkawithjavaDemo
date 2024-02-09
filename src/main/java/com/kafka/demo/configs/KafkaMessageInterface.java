package com.kafka.demo.configs;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface KafkaMessageInterface {

    void processMessage(String topicName, ConsumerRecord<String, String> message) throws Exception;
}
