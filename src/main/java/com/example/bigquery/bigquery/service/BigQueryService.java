package com.example.bigquery.bigquery.service;

import com.example.bigquery.bigquery.dto.LogsDtoNew;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.List;
import java.util.Map;

public interface BigQueryService {
    void loadDataToTable(String tableName);

    void createTable(String tableName);

    void createDatabase(String datasetName);

    List<Map<String, String>> getAllRowsFromTable(String tableName);

    void loadDataThroughStreamToTable(String tableName);

    void loadJsonDataToTable(String tableName);

    void pushToKafka(LogsDtoNew logsDtoNew) throws JsonProcessingException;

    void getdataset();

    void appendToBiquery(LogsDtoNew logsDtoNew);
}
