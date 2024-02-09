package com.kafka.demo;

import com.kafka.demo.configs.KafkaMessageInterface;
import com.kafka.demo.handlers.ConsumerHandler;
import com.kafka.demo.handlers.ProducerHandler;
import com.kafka.demo.helper.KafkaTopicHelper;
import com.kafka.demo.helper.MessageHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.util.Locale;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App
{
    private static final String TOPIC_NAME = "my_topic";
    private static class AppImpl implements KafkaMessageInterface{
        static Logger log = Logger.getLogger(AppImpl.class);
        @Override
        public void processMessage(String topicName, ConsumerRecord<String, String> message) throws Exception {
            String source = AppImpl.class.getName();
            JSONObject obj = MessageHelper.getMessageLogsAsJSON(source, message.topic(), message.key(), message.value());
            System.out.println(obj.toJSONString());
            log.info(obj.toJSONString());
        }

    }

    public static void main( String[] args ) throws Exception
    {
        String errorStr = "ERROR: You need to declare the first parameter as Producer or Consumer, " +
                "the second parameter is the topic name";

        if (args.length != 1){
            System.out.println(errorStr);
            return;
        }

        String mode = args[0];
        // String topicName = args[1];
        KafkaTopicHelper.createFixedTopic(TOPIC_NAME);
        int messageCount = 2;

        switch(mode.toLowerCase(Locale.ROOT)) {
            case "producer":
                System.out.println("Starting the Producer\n");
                new ProducerHandler().run(TOPIC_NAME, messageCount);
                break;
            case "consumer":
                System.out.println("Starting the Consumer\n");
                new ConsumerHandler().runAlways(TOPIC_NAME, new AppImpl());
                break;
            default:
                System.out.println(errorStr);
        }
    }
}
