package io.confluent.connect.http.listener;

import io.confluent.connect.http.streams.TopologyBuilder;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@Component
public class HttpSinkDemoApplicationListener {

    public static final String BOOTSTRAP_CST = "KAFKA_BOOTSTRAP_SERVER";
    public static final String TOPIC_PARTITIONS = "KAFKA_TOPIC_PARTITIONS";

    private KafkaStreams kafkaStreams;
    private TopologyBuilder builder;

    @PostConstruct
    public void onStart(){
        createTopics();

        builder = new TopologyBuilder();
        kafkaStreams = new KafkaStreams(builder.build(), streamConfig());
        kafkaStreams.start();
    }

    @PreDestroy()
    public void stop() {
        kafkaStreams.close(Duration.ofSeconds(30));
    }

    private Properties streamConfig(){
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getEnvOrDefault(BOOTSTRAP_CST, "localhost:9092"));
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ack-forwarder");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return config;
    }

    private String getEnvOrDefault(String envVar, String _default){
        String var = System.getenv(envVar);
        return var != null ? var : _default;
    }

    private void createTopics(){

        List<String> topicsToCreate = new ArrayList<>();
        List<String> topicsNeed = List.of("http-messages", "error-responses", "success-responses", "ack-status");

        Properties conf = new Properties();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getEnvOrDefault(BOOTSTRAP_CST, "localhost:9092"));
        AdminClient adminClient = KafkaAdminClient.create(conf);

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        try {
            for(String s : listTopicsResult.names().get()){
                if(topicsNeed.contains(s))
                    topicsNeed.remove(s);
            }
            topicsToCreate.addAll(topicsNeed);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if(!topicsToCreate.isEmpty()) {

            int partition = Integer.parseInt(getEnvOrDefault(TOPIC_PARTITIONS, "1"));

            List<NewTopic> newTopics = topicsToCreate.stream().map((topic) -> new NewTopic(topic, partition, (short) 1))
                    .collect(Collectors.toList());
            adminClient.createTopics(newTopics);
        }

    }
}