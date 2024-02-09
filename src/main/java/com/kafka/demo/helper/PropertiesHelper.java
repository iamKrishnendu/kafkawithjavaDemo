package com.kafka.demo.helper;

import com.kafka.demo.handlers.ProducerHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesHelper {

    public static Properties getProperties() throws Exception {
        Properties prop = null;

        try{
            InputStream input = ProducerHandler.class.getClassLoader().getResourceAsStream("config.properties");
            prop = new Properties();

            if(input == null){
                throw new Exception("Unable to read config.properties");
            }

            prop.load(input);
        }catch (IOException e){
            e.printStackTrace();
        }

        return prop;
    }
}
