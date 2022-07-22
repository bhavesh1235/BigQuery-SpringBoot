package com.example.bigquery.bigquery.controller;

import com.example.bigquery.bigquery.dto.LogsDtoNew;
import com.example.bigquery.bigquery.service.BigQueryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/bigquery")
public class BigQueryController {
    @Autowired
    BigQueryService bigQueryService;

    @GetMapping("/create/dataset/{datasetName}")
    public void createDatabase(@PathVariable("datasetName") String datasetName){
        bigQueryService.createDatabase(datasetName);
    }

    @GetMapping("/create/table/{tableName}")
    public void createTable(@PathVariable("tableName") String tableName){
        bigQueryService.createTable(tableName);
    }

    @GetMapping("/insert/{tableName}")
    public void insertDataToTable(@PathVariable("tableName") String tableName) {
        bigQueryService.loadDataToTable(tableName);
    }

    @GetMapping("/query/{tableName}")
    public ResponseEntity<List<Map<String, String>>> getQuery(@PathVariable("tableName") String tableName){
        List<Map<String, String>> logsDtoNewList = bigQueryService.getAllRowsFromTable(tableName);
        return ResponseEntity.ok(logsDtoNewList);
    }

    @GetMapping("/insert/stream/{tableName}")
    public void insertDataThroughStreamToTable(@PathVariable("tableName") String tableName) {
        bigQueryService.loadDataThroughStreamToTable(tableName);
    }

    @GetMapping("/insert/json/{tableName}")
    public void insertJsonDataToTable(@PathVariable("tableName") String tableName) {
        bigQueryService.loadJsonDataToTable(tableName);
    }

    @PostMapping("/insert/kafka")
    public void publishToKafka(@RequestBody LogsDtoNew logsDtoNew) throws JsonProcessingException {
        bigQueryService.pushToKafka(logsDtoNew);
    }

    @GetMapping("/getDataset")
    public void getData() {
        bigQueryService.getdataset();
    }
}
