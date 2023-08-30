package com.example.newkafkastreamsproject.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
public class KafkaConfiguration {

    @Bean
    public NewTopic topic1() {
        return new NewTopic("topic1", 1, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic("topic2", 1, (short) 1);
    }

    @Bean
    public NewTopic resultTopic() {
        return new NewTopic("topicResult", 1, (short) 1);
    }

    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder) {
        KStream<Integer, String> topic1 = streamsBuilder.stream("topic1");
        KStream<Integer, String> topic2 = streamsBuilder.stream("topic2");

        topic1.join(topic2, String::concat, JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5L), Duration.ZERO))
                .peek((key, value) -> System.out.println("Key: " + key + " value: " + value))
                .to("topicResult");
    }


}
