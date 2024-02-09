package com.kafka.demo.configs;

public abstract class AbstractKafka {

    public AbstractKafka(){
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                try {
                    shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public abstract void shutdown() throws Exception;

    public abstract void runAlways(String topicName, KafkaMessageInterface messageHandler) throws Exception;
}
