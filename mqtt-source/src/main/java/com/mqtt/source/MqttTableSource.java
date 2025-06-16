/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Mqtt Source connector
/
/       File            :   MqttTableSource.java
/
/       Description     :   MQTT Source connector
/
/       Created     	:   June 2025
/
/       copyright       :   Copyright 2025, - G Leonard, georgelza@gmail.com
/
/       GIT Repo        :   https://github.com/georgelza/MQTT-Flink-Source-connector.git
/
/       Blog            :   https://medium.com/p/54e3f54fd2d5
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////
package com.mqtt.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class MqttTableSource implements ScanTableSource {

    private final String   brokerURL;
    private final String   topic;
    private final String   format;
    private final String   clientId;
    private final int      qos;
    private final String   username;
    private final String   password;
    private final boolean  automaticReconnect;
    private final boolean  cleanSession;
    private final boolean  ssl;
    private final int      connect_timeout_seconds;
    private final int      keep_alive_internval_seconds;
    private final int      max_reconnect_delay_seconds;
    private final int      executor_service_threads;
    private final DataType producedDataType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    // Existing SSL parameters
    private final String sslTruststorePath;
    private final String sslTruststorePassword;
    private final String sslKeystorePath;
    private final String sslKeystorePassword;
    private final String sslKeyPassword;
    private final String sslProtocolVersion;
    private final String sslCipherSuites;

    // New Last Will parameters
    private final String lastWillTopic;
    private final String lastWillMessage;
    private final Integer lastWillQos; // Use Integer to allow for null if not set
    private final Boolean lastWillRetain; // Use Boolean to allow for null if not set


     public MqttTableSource(
            String   brokerURL,
            String   topic,
            String   format,
            String   clientId,
            int      qos,
            String   username,
            String   password,
            boolean  automaticReconnect,
            boolean  cleanSession,
            boolean  ssl,
            int      connect_timeout_seconds,
            int      keep_alive_internval_seconds,
            int      max_reconnect_delay_seconds,
            int      executor_service_threads,
            DataType producedDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            // Existing SSL parameters
            String sslTruststorePath,
            String sslTruststorePassword,
            String sslKeystorePath,
            String sslKeystorePassword,
            String sslKeyPassword,
            String sslProtocolVersion,
            String sslCipherSuites,
            // New Last Will parameters
            String lastWillTopic,
            String lastWillMessage,
            Integer lastWillQos,
            Boolean lastWillRetain) {

        this.brokerURL                      = brokerURL;
        this.topic                          = topic;
        this.format                         = format;
        this.clientId                       = clientId;
        this.qos                            = qos;
        this.username                       = username;
        this.password                       = password;
        this.automaticReconnect             = automaticReconnect;
        this.cleanSession                   = cleanSession;
        this.ssl                            = ssl;
        this.connect_timeout_seconds        = connect_timeout_seconds;
        this.keep_alive_internval_seconds   = keep_alive_internval_seconds;
        this.max_reconnect_delay_seconds    = max_reconnect_delay_seconds;
        this.executor_service_threads       = executor_service_threads;
        this.producedDataType               = producedDataType;
        this.decodingFormat                 = decodingFormat;

        // Initialize existing SSL parameters
        this.sslTruststorePath = sslTruststorePath;
        this.sslTruststorePassword = sslTruststorePassword;
        this.sslKeystorePath = sslKeystorePath;
        this.sslKeystorePassword = sslKeystorePassword;
        this.sslKeyPassword = sslKeyPassword;
        this.sslProtocolVersion = sslProtocolVersion;
        this.sslCipherSuites = sslCipherSuites;

        // Initialize new Last Will parameters
        this.lastWillTopic = lastWillTopic;
        this.lastWillMessage = lastWillMessage;
        this.lastWillQos = lastWillQos;
        this.lastWillRetain = lastWillRetain;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);

        MqttSourceFunction sourceFunction = new MqttSourceFunction(
            brokerURL,
            topic,
            format,
            clientId,
            qos,
            username,
            password,
            automaticReconnect,
            cleanSession,
            ssl,
            connect_timeout_seconds,
            keep_alive_internval_seconds,
            max_reconnect_delay_seconds,
            executor_service_threads,
            deserializer,
            sslTruststorePath,
            sslTruststorePassword,
            sslKeystorePath,
            sslKeystorePassword,
            sslKeyPassword,
            sslProtocolVersion,
            sslCipherSuites,
            // Pass new Last Will parameters to MqttSourceFunction
            lastWillTopic,
            lastWillMessage,
            lastWillQos,
            lastWillRetain
        );

        return SourceFunctionProvider.of(sourceFunction, false);
    }


    @Override
    public DynamicTableSource copy() {
        return new MqttTableSource(
            brokerURL,
            topic,
            format,
            clientId,
            qos,
            username,
            password,
            automaticReconnect,
            cleanSession,
            ssl,
            connect_timeout_seconds,
            keep_alive_internval_seconds,
            max_reconnect_delay_seconds,
            executor_service_threads,
            producedDataType,
            decodingFormat,
            sslTruststorePath,
            sslTruststorePassword,
            sslKeystorePath,
            sslKeystorePassword,
            sslKeyPassword,
            sslProtocolVersion,
            sslCipherSuites,
            // Copy new Last Will parameters
            lastWillTopic,
            lastWillMessage,
            lastWillQos,
            lastWillRetain
        );
    }

    @Override
    public String asSummaryString() {
        return "MQTT Table Source";
    }
}