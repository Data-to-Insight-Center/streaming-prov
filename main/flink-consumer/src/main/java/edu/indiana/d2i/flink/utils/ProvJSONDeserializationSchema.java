package edu.indiana.d2i.flink.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class ProvJSONDeserializationSchema implements DeserializationSchema<ObjectNode> {

    private ObjectMapper mapper;

    public ProvJSONDeserializationSchema() {
    }

    public ObjectNode deserialize(byte[] message) throws IOException {
        if (this.mapper == null) {
            this.mapper = new ObjectMapper();
        }

        return this.mapper.readValue(message, ObjectNode.class);
    }

    @Override
    public boolean isEndOfStream(ObjectNode jsonNodes) {
        return false;
    }

    public TypeInformation<ObjectNode> getProducedType() {
        return TypeExtractor.getForClass(ObjectNode.class);
    }

}
