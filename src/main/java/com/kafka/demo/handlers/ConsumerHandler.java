package com.kafka.demo.handlers;

import com.kafka.demo.configs.AbstractKafka;
import com.kafka.demo.configs.KafkaMessageInterface;
import com.kafka.demo.helper.MessageHelper;
import com.kafka.demo.helper.PropertiesHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerHandler extends AbstractKafka {

    private static final int TIME_OUT_MS = 5000;
    private KafkaConsumer<String, String> kafkaConsumer = null;
    private final AtomicBoolean close = new AtomicBoolean(false);

    static Logger log = Logger.getLogger(ConsumerHandler.class);

    public ConsumerHandler() throws Exception{}

    @Override
    public void shutdown() throws Exception {
        close.set(true);
        log.info(MessageHelper.getSimpleJSONObject("Closing Consumer instance"));
        getKafkaConsumer().wakeup();
    }


    @Override
    public void runAlways(String topicName, KafkaMessageInterface messageHandler) throws Exception {
        Properties prop = PropertiesHelper.getProperties();
        this.setKafkaConsumer(new KafkaConsumer<>(prop));

        try{
            getKafkaConsumer().subscribe(List.of(topicName));
            while(!close.get()){
                ConsumerRecords<String, String> records =
                            getKafkaConsumer().poll(Duration.ofMillis(TIME_OUT_MS));
                if(records.count() == 0){
                    log.info(MessageHelper.getSimpleJSONObject("No records are found"));
                }

                for(ConsumerRecord<String, String> record : records){
                        messageHandler.processMessage(topicName, record);
                }
            }
        }catch (WakeupException e){
                if(!close.get()) throw e;
        }
    }

    public KafkaConsumer<String, String> getKafkaConsumer(){
        return kafkaConsumer;
    }

    public void setKafkaConsumer(KafkaConsumer<String, String> kafkaConsumer){
        this.kafkaConsumer = kafkaConsumer;
    }
}
