/*
 * Copyright 2017 The Trustees of Indiana University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author isuriara@indiana.edu
 */

package edu.indiana.d2i.flink.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ProvReductionTest {

//    public static void main(String[] args) {
////        File file = new File("/Users/isuru/research/streaming-prov/flink-consumer/test1.txt");
////        File file = new File("/Users/isuru/research/streaming-prov/flink-consumer/src/main/java/edu/indiana/d2i/flink/test/working1.txt");
//        File file = new File("/Users/isuru/research/streaming-prov/flink-consumer/src/main/java/edu/indiana/d2i/flink/test/test-global.txt");
////        File file = new File("/Users/isuru/research/streaming-prov/flink-consumer/src/main/java/edu/indiana/d2i/flink/test/2811-1");
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


    public static void main(String[] args) {
        File file = new File("/Users/isuru/research/streaming-prov/flink-consumer/" +
                "src/main/java/edu/indiana/d2i/flink/test/horiz");
        ObjectMapper mapper = new ObjectMapper();
        ProvState processor = new ProvState();
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                processor.processNotification((ObjectNode) mapper.readTree(scanner.nextLine()));
            }
            scanner.close();
            processor.printState();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        int x = StringUtils.countMatches("foo_223_sdfd", "_");
//        System.out.println(x);
//    }



}
