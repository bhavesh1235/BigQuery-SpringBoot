package com.example.bigquery.bigquery.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogsDto {
    @JsonProperty("userEmailId")
    private String userEmailId;
    @JsonProperty("customerEmailId")
    private String customerEmailId;
    @JsonProperty("skipLog")
    private Boolean skipLog;
    @JsonProperty("additionalInfo")
    private String additionalInfo;
    @JsonProperty("useCase")
    private String useCase;
    @JsonProperty("timestamp")
    private String timestamp;
}
