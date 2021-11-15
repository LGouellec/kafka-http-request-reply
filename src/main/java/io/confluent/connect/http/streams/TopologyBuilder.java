package io.confluent.connect.http.streams;

import io.confluent.connect.http.streams.beans.AckMessage;
import io.confluent.connect.http.streams.beans.IntermediateMessage;
import io.confluent.connect.http.streams.serde.AckMessageSerde;
import io.confluent.connect.http.streams.serde.IntermediateMessageSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class TopologyBuilder {

    public Topology build() {

        Serdes.StringSerde stringSerde = new Serdes.StringSerde();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, IntermediateMessage> sucessResponses = builder
                .stream("success-responses", Consumed.with(stringSerde, stringSerde))
                .mapValues((v) -> new IntermediateMessage(v, true));

        KStream<String, IntermediateMessage> errorResponses = builder
                .stream( "error-responses", Consumed.with(stringSerde, stringSerde))
                .mapValues((v) -> new IntermediateMessage(v, false));

        KStream<String, IntermediateMessage> mergeResponses = sucessResponses.merge(errorResponses);

        KStream<String, String> sourceStream = builder
                .stream("http-messages", Consumed.with(stringSerde, stringSerde));

        sourceStream.join(mergeResponses,
                        (sourceMessage, responseMessage) -> {
                            AckMessage ackMessage = new AckMessage();
                            ackMessage.setSourceMessage(sourceMessage);
                            ackMessage.setResponseMessage(responseMessage.getMessage());
                            ackMessage.setError(responseMessage.isError());
                            ackMessage.setSucess(responseMessage.isSucess());
                            ackMessage.setAckStatus(
                                    responseMessage.isSucess() && responseMessage.getMessage().contains("OK"));
                            return ackMessage;
                        },
                        JoinWindows.of(Duration.ofMinutes(1)),
                        StreamJoined.with(stringSerde, stringSerde, new IntermediateMessageSerde()))
                .to("ack-status", Produced.with(stringSerde, new AckMessageSerde()));

        return builder.build();
    }
}
