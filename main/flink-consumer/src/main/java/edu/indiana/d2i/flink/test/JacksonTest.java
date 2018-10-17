package edu.indiana.d2i.flink.test;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JacksonTest {

    public static void main(String[] args) throws IOException {
        String key = "k\\@";
        if (key.contains("\""))
            key = key.replace("\"", "");
        if (key.contains("\\"))
            key = key.replace("\\", "");

        String jsonString = "{\"" + key + "\":\"v1\",\"k2\":\"v2\"}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);

        System.out.println(actualObj.toString());
    }
}
