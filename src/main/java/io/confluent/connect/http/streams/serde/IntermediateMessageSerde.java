package io.confluent.connect.http.streams.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.http.streams.beans.AckMessage;
import io.confluent.connect.http.streams.beans.IntermediateMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;


public class IntermediateMessageSerde implements Serde<IntermediateMessage> {

    private final ObjectMapper mapper;

    public IntermediateMessageSerde(){
        mapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<IntermediateMessage> serializer() {

        return new Serializer<IntermediateMessage>(){
            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public byte[] serialize(String s, IntermediateMessage intermediateMessage) {
                try {
                    return mapper.writeValueAsBytes(intermediateMessage);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public Deserializer<IntermediateMessage> deserializer() {
        return new Deserializer<IntermediateMessage>(){

            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public IntermediateMessage deserialize(String s, byte[] bytes) {
                try {
                    return mapper.readValue(bytes, IntermediateMessage.class);
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() {

            }
        };
    }
}
