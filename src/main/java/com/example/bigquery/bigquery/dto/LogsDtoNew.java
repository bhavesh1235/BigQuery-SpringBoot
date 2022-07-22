package com.example.bigquery.bigquery.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogsDtoNew {
    private Integer id;

    private String agentEmailId;

    private String userAccountId;

    private String eventName;

    private String metadata;

    private String requestBody;

    private String responseBody;

    private String createdAt;

}
