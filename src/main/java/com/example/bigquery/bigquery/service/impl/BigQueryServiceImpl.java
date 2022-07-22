package com.example.bigquery.bigquery.service.impl;

import com.example.bigquery.bigquery.constants.Constants;
import com.example.bigquery.bigquery.dto.LogsDtoNew;
import com.example.bigquery.bigquery.kafka.KafkaMessageProducer;
import com.example.bigquery.bigquery.service.BigQueryService;
import com.example.bigquery.bigquery.utils.DataWriter;
import com.example.bigquery.bigquery.utils.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class BigQueryServiceImpl implements BigQueryService {
    @Autowired
    BigQuery bigQuery;
    @Autowired
    DataWriter writer;
    @Autowired
    ObjectMapper objectMapper;
    private Integer offset =0;
    JSONArray jsonArray = new JSONArray();
    @Autowired
    BigQueryWriteClient bigQueryWriteClient;
    @Autowired
    KafkaMessageProducer kafkaMessageProducer;
    private String datasetName = "sample";

    private Long temp = 1000L;

    public void createDatabase(String datasetName) {
        try {
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
            Dataset newDataset = bigQuery.create(datasetInfo);
            String newDatasetName = newDataset.getDatasetId().getDataset();
            log.info(newDatasetName + " created successfully");
            this.datasetName = datasetName;
        } catch (BigQueryException e) {
            log.info("Dataset was not created. " + e);
        }
    }

    public void createTable(String tableName) {
        Schema schema = Utils.getSchema();
        try {
            TableId tableId = TableId.of(datasetName, tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            bigQuery.create(tableInfo);
            log.info("Table created successfully : {}", tableInfo);
        } catch (BigQueryException e) {
            log.info("Table was not created. " + e);
        }
    }

    public void loadDataToTable(String tableName) {

        Schema schema = Utils.getSchema();
        try {
            TableId tableId = TableId.of(datasetName, tableName);

            WriteChannelConfiguration writeChannelConfiguration =
                    WriteChannelConfiguration.newBuilder(tableId).setSchema(schema).setAutodetect(true).setFormatOptions(FormatOptions.csv()).build();

            // The location and JobName must be specified; other fields can be auto-detected.
            String jobName = "jobId_" + UUID.randomUUID().toString();
            JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();

            // Imports a local file into a table.
            try (TableDataWriteChannel writer = bigQuery.writer(jobId, writeChannelConfiguration);
                 OutputStream stream = Channels.newOutputStream(writer)) {
                Files.copy(Path.of(Constants.CSV_PATH), stream);
            }
//            InsertAllResponse insertAllResponse = bigQuery.insertAll(InsertAllRequest.newBuilder().build());
            // Get the Job created by the TableDataWriteChannel and wait for it to complete.
            Job job = bigQuery.getJob(jobId);
            Job completedJob = job.waitFor();
            if (completedJob == null) {
                log.info("Job not executed since it no longer exists.");
                return;
            } else if (completedJob.getStatus().getError() != null) {
                log.info(
                        "BigQuery was unable to load local file to the table due to an error: "
                                + job.getStatus().getError());
                return;
            }

            // Get output status
            JobStatistics.LoadStatistics stats = job.getStatistics();
            log.info("Successfully loaded {} rows", stats.getOutputRows());
        } catch (BigQueryException | IOException | InterruptedException e) {
            log.info("Local file not loaded. " + e);
        }
    }

    @Override
    public List<Map<String, String>> getAllRowsFromTable(String tableName) {
        List<LogsDtoNew> logsDtoList = new ArrayList<>();
        List<Map<String, String>> ansList = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", Constants.DATASET_NAME, Constants.TABLE_NAME);
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryJobConfiguration config = QueryJobConfiguration.newBuilder(query).build();
            TableResult results = bigQuery.query(config);

            //get fields list
            FieldList fieldsList = results.getSchema().getFields();
            //iterate through all rows returned in results
            for (FieldValueList row : results.iterateAll()) {
                Map<String, String> employeeRowMap = new HashMap<>();
                int index = 0;
                //iterate in single row and extract values from it
                for (FieldValue value : row) {
                    if(!value.isNull()) {
                        employeeRowMap.put(fieldsList.get(index).getName(), value.getValue().toString());
                    }
                    index += 1;
                }
                //Map to DTO Conversion
                LogsDtoNew logsDtoNew = objectMapper.convertValue(employeeRowMap, LogsDtoNew.class);
                logsDtoList.add(logsDtoNew);
                ansList.add(employeeRowMap);
//                employeeRowMap.
            }
            log.info("Query to get Employee List : {}", logsDtoList);
        } catch (BigQueryException | InterruptedException e) {
            log.info("Query not performed " + e);
        }
        return ansList;
    }

    @Override
    public void loadDataThroughStreamToTable(String tableName) {
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("Id", "6");
        rowContent.put("Name", "Abc");
        rowContent.put("Age", 22);
        rowContent.put("Salary", 200);
        rowContent.put("Gender", "Male");

        try {
            TableId tableId = TableId.of(datasetName, tableName);

            InsertAllResponse response =
                    bigQuery.insertAll(
                            InsertAllRequest.newBuilder(tableId)
                                    .addRow(rowContent)
                                    .build());

            if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    log.info("Response error: " + entry.getValue());
                }
            }
            log.info("Rows successfully inserted into table");
        } catch (BigQueryException e) {
            log.info("Insert operation not performed " + e);
        }
    }

    @Override
    public void loadJsonDataToTable(String tableName) {
        try {
            runWritePendingStream();
        } catch (InterruptedException | IOException | Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }
    public void runWritePendingStream()
            throws Descriptors.DescriptorValidationException, InterruptedException, IOException {
        String projectId = Constants.PROJECT_ID;
        String datasetName = Constants.DATASET_NAME;
        String tableName = Constants.TABLE_NAME;
        writePendingStream(projectId, datasetName, tableName);
    }

    public void writePendingStream(String projectId, String datasetName, String tableName)
            throws Descriptors.DescriptorValidationException, InterruptedException, IOException {
        TableName parentTable = TableName.of(projectId, datasetName, tableName);

        // One time initialization.
//        writer.initialize(parentTable);

        appendRows();

        // Final cleanup for the stream.
        writer.cleanup();

        log.info("Appended records successfully.");

        batchCommitStream( parentTable);
    }
    public void appendRows(){
        try {
            // Write two batches of fake data to the stream, each with 10 JSON records.
            // Data may be batched up to the maximum request size:
//            long offset = 0;
//            for (int i = 0; i < 2; i++) {
//                // Create a JSON object that is compatible with the table schema.
//                JSONArray jsonArr = new JSONArray();
//                for (int j = 0; j < 10; j++) {
//                    JSONObject record = new JSONObject();
//                    record.put("col1", String.format("batch-record %03d-%03d", i, j));
//                    jsonArr.put(record);
//                }
//                writer.append(jsonArr, offset);
//                offset += jsonArr.length();
//            }
            writer.append(Utils.getJsonArray(), 0);
        } catch (ExecutionException | Descriptors.DescriptorValidationException | IOException e) {
            // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
            // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information, see:
            log.info("Failed to append records. " + e);
        }
    }
    public void batchCommitStream(TableName parentTable){
        // Once all streams are done, if all writes were successful, commit all of them in one request.
        // This example only has the one stream. If any streams failed, their workload may be
        // retried on a new stream, and then only the successful stream should be included in the
        // commit.
        BatchCommitWriteStreamsRequest commitRequest =
                BatchCommitWriteStreamsRequest.newBuilder()
                        .setParent(parentTable.toString())
                        .addWriteStreams(writer.getStreamName())
                        .build();
        BatchCommitWriteStreamsResponse commitResponse = bigQueryWriteClient.batchCommitWriteStreams(commitRequest);
        // If the response does not have a commit time, it means the commit operation failed.
        if (!commitResponse.hasCommitTime()) {
            for (StorageError err : commitResponse.getStreamErrorsList()) {
                log.info(err.getErrorMessage());
            }
            throw new RuntimeException("Error committing the streams");
        }
        log.info("Appended and committed records successfully.");
    }

    @Override
    public void pushToKafka(LogsDtoNew logsDtoNew) throws JsonProcessingException {
        kafkaMessageProducer.sendLogsToTopic(logsDtoNew);
    }

    @Override
    public void getdataset() {
        DatasetId datasetId = DatasetId.of(Constants.PROJECT_ID, Constants.DATASET_NAME);
        Dataset dataset = bigQuery.getDataset(datasetId);
log.info("{}",dataset);
Page<Table> tables =  bigQuery.listTables(datasetId, BigQuery.TableListOption.pageSize(10));
        // View dataset properties
        String description = String.valueOf(dataset.getCreationTime());
        tables.iterateAll().forEach(table -> System.out.print(table.getTableId().getTable() + "\n"));
        TableName parentTable = TableName.of(Constants.PROJECT_ID, Constants.DATASET_NAME, Constants.TABLE_NAME);

        System.out.println(description);
    }

    @Override
    public void appendToBiquery(LogsDtoNew logsDtoNew) {

        try {
            String jsonString = objectMapper.writeValueAsString(logsDtoNew);
            JSONObject jsonObject = new JSONObject(jsonString);
            jsonArray.put(jsonObject);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        log.info("{}",jsonArray);
        offset++;
        if(offset == 2){
            try {
                log.info("initiation");
                TableName parentTable = TableName.of(Constants.PROJECT_ID, Constants.DATASET_NAME, Constants.TABLE_NAME);

                writer.initialize(parentTable);

                writer.append(jsonArray, 0);
                log.info("Appended records successfully.");


                writer.cleanup();
                log.info("Writer cleaned up successfully.");



                batchCommitStream(parentTable);
                log.info("committed all Records");

                offset=0;
                jsonArray = new JSONArray();

            } catch (Descriptors.DescriptorValidationException | IOException | ExecutionException |
                     InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


}


