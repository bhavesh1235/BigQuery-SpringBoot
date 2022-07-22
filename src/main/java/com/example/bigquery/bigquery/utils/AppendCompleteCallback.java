package com.example.bigquery.bigquery.utils;

import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
@Slf4j

public class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
    @Autowired
    private final DataWriter parent;

    public AppendCompleteCallback(DataWriter parent) {
        this.parent = parent;
    }

    public void onSuccess(AppendRowsResponse response) {
        log.info("Append {} success", response.getAppendResult().getOffset().getValue());
        done();
    }

    public void onFailure(Throwable throwable) {
        synchronized (this.parent.lock) {
            if (this.parent.error == null) {
                Exceptions.StorageException storageException = Exceptions.toStorageException(throwable);
                this.parent.error =
                        (storageException != null) ? storageException : new RuntimeException(throwable);
            }
        }
        log.info("Error: " + throwable.toString());
        done();
    }

    private void done() {
        // Reduce the count of in-flight requests.
        this.parent.inFlightRequestCount.arriveAndDeregister();
    }
}
