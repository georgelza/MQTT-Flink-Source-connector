/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Mqtt Source connector
/
/       File            :   MqttJob.java
/
/       Description     :   MQTT Source connector
/
/       Created     	:   April 2025
/
/       copyright       :   "Copyright 2024, - G Leonard"
/                       
/       GIT Repo        :
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////
package com.mqtt.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table; 
import org.apache.flink.table.catalog.ResolvedSchema; 
import org.apache.flink.table.catalog.Column; 

import org.slf4j.Logger; 
import org.slf4j.LoggerFactory; 

import java.util.stream.Collectors; 

/**
 * Main Flink Job class to define and execute the Table API pipeline.
 * This job expects the source and sink tables to be defined externally via Flink SQL DDL.
 */
public class MqttJob {

    private static final Logger LOG = LoggerFactory.getLogger(MqttJob.class); // Initialize logger

    public static void main(String[] args) throws Exception {
        // Setup streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Setup table environment with Blink planner in streaming mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // The source and sink table names are expected to be defined externally (via SQL DDL).
        // This job will only execute the INSERT INTO statement.
        // Ensure that 'mqtt_source_table' and 'my_print_sink' are created before running this job.

        // Get the source table
        Table sourceTable = tableEnv.from("mqtt_source_table");

        // Get the schema of the source table
        // This allows us to dynamically get column names
        ResolvedSchema sourceSchema = sourceTable.getResolvedSchema();

        // Dynamically get all physical column names from the source table
        String selectColumns = sourceSchema.getColumns().stream()
                .filter(Column::isPhysical) // Filter for physical columns only
                .map(Column::getName)
                .collect(Collectors.joining(", "));

        // Construct the generic INSERT INTO query
        String insertQuery = String.format("INSERT INTO my_print_sink SELECT %s FROM mqtt_source_table", selectColumns);
        LOG.info("Executing insert query: {}", insertQuery); // Use logger

        TableResult result = tableEnv.executeSql(insertQuery);

        // Print job results or wait for job completion
        result.await(); // Wait for the job to finish (or run continuously for streaming)
        LOG.info("Flink job finished."); // Use logger
    }
}