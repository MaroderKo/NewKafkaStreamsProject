package com.example.newkafkastreamsproject.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaService {
    private final KafkaTemplate<Object, Object> template;

    public void sendMessage(String topic, String data) {
        template.send(topic, 15, data).thenAccept(result -> log.info(topic + " " + result.getRecordMetadata().offset() + "=" + result.getProducerRecord().value()));
    }
}
