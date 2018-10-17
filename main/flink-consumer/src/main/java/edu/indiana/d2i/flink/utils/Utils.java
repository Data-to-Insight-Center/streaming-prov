package edu.indiana.d2i.flink.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Utils {

    public static Properties loadPropertiesFromFile() {
        Properties properties = new Properties();
        try {
//            properties.load(new FileInputStream("/home/isurues/flink/kafka.properties"));
            properties.load(new FileInputStream("/home/isuru/2018thesiswork/checkouts/flink-1.5-consumer/kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}
