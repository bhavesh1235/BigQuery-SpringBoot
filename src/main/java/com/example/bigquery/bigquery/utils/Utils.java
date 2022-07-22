package com.example.bigquery.bigquery.utils;

import com.example.bigquery.bigquery.dto.EmployeeDto;
import com.example.bigquery.bigquery.dto.LogsDtoNew;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.json.JSONArray;
import org.json.JSONObject;

public class Utils {
    public static Schema getSchema(){
        return Schema.of(
                Field.of("Id", StandardSQLTypeName.STRING),
                Field.of("Name", StandardSQLTypeName.STRING),
                Field.of("Age", StandardSQLTypeName.NUMERIC),
                Field.of("Salary", StandardSQLTypeName.INT64),
                Field.of("Gender", StandardSQLTypeName.STRING));
    }
    public static JSONArray getJsonArray(){
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", 2);
        jsonObject.put("user_account_id", "55654");
        jsonObject.put("agent_email_id", "manoj@gmail.com");
        jsonObject.put("event_name", "KYC_APPROVED_BY");
        jsonObject.put("meta_data", "2022-06-21");
        jsonArray.put(jsonObject);
//        LogsDtoNew emp1 = LogsDtoNew.builder().id(1).userAccountId("11221")
//                .agentEmailId("anil@gmail.com")
//                .eventName("KYC_APPROVED_BY")
//                .metadata("2022-06-20").build();
//        LogsDtoNew emp2 = LogsDtoNew.builder().id(2).name("Gfhg").age(28).gender("Female").salary(4321).build();
//        jsonArray.put(emp1);
//        jsonArray.put(emp2);
        return jsonArray;
    }
}

