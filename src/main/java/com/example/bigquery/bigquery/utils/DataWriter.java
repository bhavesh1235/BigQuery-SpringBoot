package com.example.bigquery.bigquery.utils;

import com.example.bigquery.bigquery.constants.Constants;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
@Component
@Slf4j
public class DataWriter {
    private JsonStreamWriter streamWriter;
    // Track the number of in-flight requests to wait for all responses before shutting down.
    final Phaser inFlightRequestCount = new Phaser(1);

    final Object lock = new Object();
    @Autowired
    BigQueryWriteClient bigQueryWriteClient;
    @GuardedBy("lock")
    RuntimeException error = null;

    public void initialize(TableName parentTable)
            throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        // Initialize write stream for the specified table.
        WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();

        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(parentTable.toString())
                        .setWriteStream(stream)
                        .build();
        WriteStream writeStream = bigQueryWriteClient.createWriteStream(createWriteStreamRequest);

        // Use the JSON stream writer to send records in JSON format.
        val credentials = ServiceAccountCredentials.fromStream(new FileInputStream(Constants.GCP_SERVICE_ACCOUNT));
        streamWriter = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build();
    }

    public void append(JSONArray data, long offset)
            throws Descriptors.DescriptorValidationException, IOException, ExecutionException {
        synchronized (this.lock) {
            // If earlier appends have failed, we need to reset before continuing.
            if (this.error != null) {
                throw this.error;
            }
        }
        // Append asynchronously for increased throughput.
        ApiFuture<AppendRowsResponse> future = streamWriter.append(data, offset);
        ApiFutures.addCallback(future, new AppendCompleteCallback(this), MoreExecutors.directExecutor());

        // Increase the count of in-flight requests.
        inFlightRequestCount.register();
    }

    public void cleanup() {
        // Wait for all in-flight requests to complete.
        inFlightRequestCount.arriveAndAwaitAdvance();

        // Close the connection to the server.
        streamWriter.close();

        // Verify that no error occurred in the stream.
        synchronized (this.lock) {
            if (this.error != null) {
                throw this.error;
            }
        }

        // Finalize the stream.
        FinalizeWriteStreamResponse finalizeResponse =
            bigQueryWriteClient.finalizeWriteStream(streamWriter.getStreamName());
        log.info("Rows written: " + finalizeResponse.getRowCount());
    }

    public String getStreamName() {
        return streamWriter.getStreamName();
    }
}
