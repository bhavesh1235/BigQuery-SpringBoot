package com.example.bigquery.bigquery.kafka;

import com.example.bigquery.bigquery.dto.LogsDto;
import com.example.bigquery.bigquery.dto.LogsDtoNew;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaMessageProducer {
    @Value("${kafka_topic}")
    private String kafkaTopic;
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendLogsToTopic(LogsDtoNew logsDtoNew) throws JsonProcessingException {
        log.info("Produced log to topic : {}", logsDtoNew);

        ObjectMapper objectMapper = new ObjectMapper();

//            for(int i =0 ;i<1; i++)
//                kafkaTemplate.send(kafkaTopic, objectMapper.writeValueAsString(i));
            kafkaTemplate.send(kafkaTopic,objectMapper.writeValueAsString(logsDtoNew));
        }
}

