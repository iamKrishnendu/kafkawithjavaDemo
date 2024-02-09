package com.kafka.demo.handlers;

import com.kafka.demo.configs.AbstractKafka;
import com.kafka.demo.configs.KafkaMessageInterface;
import com.kafka.demo.helper.MessageHelper;
import com.kafka.demo.helper.PropertiesHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerHandler extends AbstractKafka {

    private KafkaProducer<String, String> kafkaProducer;
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final Logger log = Logger.getLogger(ProducerHandler.class);

    public ProducerHandler() throws Exception{}
    @Override
    public void shutdown() throws Exception {
        close.set(true);
        log.info(MessageHelper.getSimpleJSONObject("Closing Producer instance"));
        this.getKafkaProducer().close();
    }

    @Override
    public void runAlways(String topicName, KafkaMessageInterface messageHandler) throws Exception {
        while (true){
            String key = UUID.randomUUID().toString();
            String randomMessage = MessageHelper.generateRandomString();

            this.sendMessage(topicName, key, randomMessage);
            Thread.sleep(500);
        }
    }

    public void run(String topicName, int numberOfMessages) throws Exception {
        int i = 0;
        while (i <= numberOfMessages) {
            String key = UUID.randomUUID().toString();
            String message = MessageHelper.generateRandomString();
            this.sendMessage(topicName, key, message);
            i++;
            Thread.sleep(100);
        }
        this.shutdown();
    }

    public void sendMessage(String topic, String key, String message) throws Exception{
        String source = ProducerHandler.class.getName();

        JSONObject obj = MessageHelper.getMessageLogsAsJSON(source, topic, key, message);
        log.info(obj.toJSONString());

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, key, message);

        getKafkaProducer().send(producerRecord);
    }

    private KafkaProducer<String, String> getKafkaProducer() throws Exception{
            if(this.kafkaProducer == null){
                Properties prop = PropertiesHelper.getProperties();
                this.kafkaProducer = new KafkaProducer<>(prop);
            }
            return this.kafkaProducer;
    }
}
