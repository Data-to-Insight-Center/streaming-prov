package edu.indiana.d2i.flink.test;

import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ProvReductionTest {

//    public static void main(String[] args) {
////        File file = new File("/Users/isuru/research/flink-1.5-consumer/test1.txt");
////        File file = new File("/Users/isuru/research/flink-1.5-consumer/src/main/java/edu/indiana/d2i/flink/test/working1.txt");
//        File file = new File("/Users/isuru/research/flink-1.5-consumer/src/main/java/edu/indiana/d2i/flink/test/test-global.txt");
////        File file = new File("/Users/isuru/research/flink-1.5-consumer/src/main/java/edu/indiana/d2i/flink/test/2811-1");
//        ObjectMapper mapper = new ObjectMapper();
//        ProvState processor = new ProvState();
//        try (Scanner scanner = new Scanner(file)) {
//            while (scanner.hasNextLine()) {
//                processor.processNotification((ObjectNode) mapper.readTree(scanner.nextLine()));
//            }
//            scanner.close();
//            processor.printState();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


//    public static void main(String[] args) {
//        File file = new File("/Users/isuru/research/flink-1.5-consumer/" +
//                "src/main/java/edu/indiana/d2i/flink/test/horiz");
//        ObjectMapper mapper = new ObjectMapper();
//        ProvState processor = new ProvState();
//        try (Scanner scanner = new Scanner(file)) {
//            while (scanner.hasNextLine()) {
//                processor.processNotification((ObjectNode) mapper.readTree(scanner.nextLine()));
//            }
//            scanner.close();
//            processor.printState();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

//    public static void main(String[] args) {
//        int x = StringUtils.countMatches("foo_223_sdfd", "_");
//        System.out.println(x);
//    }


    public static void main(String[] args) {
//        String s = "testing        hash #tags         counting #logic in java #yay";
//        String[] tags = s.toLowerCase().split("\\s+");
//        for (String t : tags) {
//            System.out.println(t);
//        }

//        int n = 0;
//
//        String g = "global";
//        String x = g + (n % 2);
//        System.out.println(x);


        String s = "partition";
        for (int i = 0; i < 20; i++) {
            String p = s + i + UUID.randomUUID().toString();
            int bucket = p.hashCode() % 20;
            System.out.println(p + ":" + bucket);
        }

    }


}
