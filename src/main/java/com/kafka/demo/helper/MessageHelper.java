package com.kafka.demo.helper;

import org.apache.commons.lang3.RandomStringUtils;
import org.json.simple.JSONObject;

import java.util.Properties;

public class MessageHelper {

    private static Properties props;

    protected static Properties getProperties() throws Exception{
        if(props == null){
            props = PropertiesHelper.getProperties();
        }

        return props;
    }

   public static JSONObject getSimpleJSONObject(String message){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("message",message);
        return jsonObject;
   }

    public static JSONObject getMessageLogsAsJSON(String source, String topic, String key, String message) throws Exception{
        JSONObject obj = new JSONObject();
        String bootstrapServer = getProperties().getProperty("bootstrap.servers");
        obj.put("bootstrapServer", bootstrapServer);
        obj.put("source", source);
        obj.put("topic", topic);
        obj.put("key", key);
        obj.put("message", message);
        return obj;
    }

    public static String generateRandomString(){
        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        String generatedString = RandomStringUtils.random(length, useLetters, useNumbers);
        return generatedString;
    }
}
