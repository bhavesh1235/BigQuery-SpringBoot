package com.example.bigquery.bigquery.kafka;

import com.example.bigquery.bigquery.dto.LogsDtoNew;
import com.example.bigquery.bigquery.service.BigQueryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class KafkaMessageListener {
    @Autowired
    BigQueryService bigQueryService;

//    @KafkaListener(groupId = "${consumer_group_id}", topics = "${kafka_topic}", containerFactory = "KafkaListenerContainerFactory")
    public void fetchLogsFromTopic(@Payload String logRecord) throws JsonProcessingException {
//                                   @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
//                                   @Header(KafkaHeaders.OFFSET) List<Long> offsets

//        log.info("Receiving Batch Logs Started");
//        for(int i=0 ; i < logs.size() ; i++){
//            log.info("Received logs = '{}' with partition-offset= '{}' ", logs.get(i),
//                    partitions.get(i) + "-" + offsets.get(i));

        log.info("All Batch Logs {}", logRecord);
    ObjectMapper objectMapper = new ObjectMapper();
    LogsDtoNew logsDtoNew = objectMapper.readValue(logRecord , LogsDtoNew.class);

//    bigQueryService.appendToBiquery(logsDtoNew);
    }
}
