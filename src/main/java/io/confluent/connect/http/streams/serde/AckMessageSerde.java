package io.confluent.connect.http.streams.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.http.streams.beans.AckMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class AckMessageSerde implements Serde<AckMessage> {

    private final ObjectMapper mapper;

    public AckMessageSerde(){
        mapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<AckMessage> serializer() {

        return new Serializer<AckMessage>(){
            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public byte[] serialize(String s, AckMessage ackMessage) {
                try {
                    return mapper.writeValueAsBytes(ackMessage);
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
    public Deserializer<AckMessage> deserializer() {
        return new Deserializer<AckMessage>(){

            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public AckMessage deserialize(String s, byte[] bytes) {
                try {
                    return mapper.readValue(bytes, AckMessage.class);
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
