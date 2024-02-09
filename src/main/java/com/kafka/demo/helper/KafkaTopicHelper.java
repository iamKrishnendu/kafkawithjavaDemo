package com.kafka.demo.helper;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.Properties;

public class KafkaTopicHelper {

    public static TopicListing createFixedTopic(String topicName) throws Exception{
        Properties props = PropertiesHelper.getProperties();
        Admin admin = Admin.create(props);

        ListTopicsResult topics = admin.listTopics();

        for(TopicListing listing : topics.listings().get()){
                if(new String(listing.name()).equals(topicName))return listing;
        }

        int partition = 1;
        short replicationFactor = 1;

        NewTopic topic = new NewTopic(topicName, partition, replicationFactor);
        CreateTopicsResult result = admin.createTopics(
                Collections.singleton(topic)
        );

        KafkaFuture<Void> future =result.values().get(topicName);
        future.get();

        topics = admin.listTopics();

        for (TopicListing listing : topics.listings().get()) {
            if(new String(listing.name()).equals(topicName)) return listing;
        }

        return null;
    }
}
