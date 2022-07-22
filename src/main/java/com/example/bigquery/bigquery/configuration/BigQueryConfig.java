package com.example.bigquery.bigquery.configuration;

import com.example.bigquery.bigquery.constants.Constants;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import lombok.val;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
@Configuration
public class BigQueryConfig {
    @Bean
    public BigQuery connectBigQuery() throws IOException {
        val credentials = ServiceAccountCredentials.fromStream(new FileInputStream(Constants.GCP_SERVICE_ACCOUNT));
        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }
    @Bean
    public BigQueryWriteClient bigQueryWriteClient() throws IOException {
        val credentials = ServiceAccountCredentials.fromStream(new FileInputStream(Constants.GCP_SERVICE_ACCOUNT));
        BigQueryWriteSettings bigQueryWriteSettings =
                BigQueryWriteSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .build();
        return BigQueryWriteClient.create(bigQueryWriteSettings);
    }

//    @Bean
//    public JsonStreamWriter jsonStreamWriter() throws IOException {
//        TableName parentTable = TableName.of(Constants.PROJECT_ID, Constants.DATASET_NAME, Constants.TABLE_NAME);
//        WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();
//
//        CreateWriteStreamRequest createWriteStreamRequest =
//                CreateWriteStreamRequest.newBuilder()
//                        .setParent(parentTable.toString())
//                        .setWriteStream(stream)
//                        .build();
//        WriteStream writeStream = null;
//        try {
//            writeStream = bigQueryWriteClient().createWriteStream(createWriteStreamRequest);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        // Use the JSON stream writer to send records in JSON format.
//        val credentials = ServiceAccountCredentials.fromStream(new FileInputStream(Constants.GCP_SERVICE_ACCOUNT));
//        try {
//            return JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).setCredentialsProvider(FixedCredentialsProvider.create(credentials))
//                    .build();
//        } catch (Descriptors.DescriptorValidationException | IOException | InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
