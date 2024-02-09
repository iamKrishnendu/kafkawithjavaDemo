package com.kafka.demo.handlers;

import com.kafka.demo.configs.KafkaMessageInterface;
import com.kafka.demo.helper.MessageHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

public class KafkaMessageInterfaceImpl implements KafkaMessageInterface {

   static Logger log = Logger.getLogger(KafkaMessageInterfaceImpl.class);
    @Override
    public void processMessage(String topicName, ConsumerRecord<String, String> message) throws Exception {
        String source =KafkaMessageInterfaceImpl.class.getName();
        JSONObject obj = MessageHelper.getMessageLogsAsJSON(source, topicName, message.key(), message.value());
        log.info(obj.toJSONString());
    }
}
