package com.example.newkafkastreamsproject.controller;

import com.example.newkafkastreamsproject.service.KafkaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("kafka/")
@RequiredArgsConstructor
@Slf4j
public class MainController {

    private final KafkaService service;

    @PostMapping("{topic}")
    private void sendMessage(@RequestBody String data, @PathVariable String topic) {
        service.sendMessage(topic, data);
    }

    @KafkaListener(id = "topicResultListener", topics = "topicResult")
    public void topicResultListener(String message) {
        log.info("topicResult: " + message);
    }

}
