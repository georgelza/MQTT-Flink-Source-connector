/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Mqtt Source connector
/
/       File            :   MqttSourceFunction.java
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
package com.mqtt.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.eclipse.paho.client.mqttv3.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class MqttSourceFunction extends RichSourceFunction<RowData> {

   // Declare a logger instance
    private static final Logger LOG = LoggerFactory.getLogger(MqttSourceFunction.class);

    private volatile boolean isRunning = true;
    private final String     brokerURL;
    private final String     Stringtopic; // Consider renaming to 'topic' for consistency
    private final String     clientId;
    private final int        qos;
    private final String     username;
    private final String     password;
    private final boolean    automaticReconnect;
    private final boolean    cleanSession;
    private final String     format;
    private final boolean    ssl;
    private final int        connect_timeout_seconds;
    private final int        keep_alive_internval_seconds;
    private final int        max_reconnect_delay_seconds;
    private final int        executor_service_threads;
    private transient MqttClient client;
    private final LinkedBlockingQueue<byte[]> messageQueue;
    private final DeserializationSchema<RowData> deserializer;

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
    private final Integer lastWillQos; // Can be Integer because it's passed from factory.defaultValue(1)
    private final Boolean lastWillRetain; // Can be Boolean because it's passed from factory.defaultValue(false)


    public MqttSourceFunction(
        String  brokerURL,
        String  topic,
        String  format,
        String  clientId,
        int     qos,
        String  username,
        String  password,
        boolean automaticReconnect,
        boolean cleanSession,
        boolean ssl,
        int     connect_timeout_seconds,
        int     keep_alive_internval_seconds,
        int     max_reconnect_delay_seconds,
        int     executor_service_threads,
        DeserializationSchema<RowData> deserializer,
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
            this.Stringtopic                    = topic;
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
            this.messageQueue                   = new LinkedBlockingQueue<>();
            this.deserializer                   = deserializer;

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
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        client = new MqttClient(brokerURL, clientId);
        MqttConnectOptions options = new MqttConnectOptions();

        if (username != null && !username.isEmpty()) {
            options.setUserName(username);
        }
        if (password != null && !password.isEmpty()) {
            options.setPassword(password.toCharArray());
        }

        // Configure SSL if enabled
        if (ssl) {
            try {
                SSLSocketFactory sslSocketFactory = createSslSocketFactory(
                    sslTruststorePath, sslTruststorePassword,
                    sslKeystorePath, sslKeystorePassword, sslKeyPassword,
                    sslProtocolVersion, sslCipherSuites);
                options.setSocketFactory(sslSocketFactory);
            } catch (Exception e) {
                LOG.error("Failed to configure SSL for MQTT client: {}", e.getMessage(), e);
                throw new MqttException(e);
            }
        }

        // Configure Last Will and Testament if topic is provided
        if (lastWillTopic != null && !lastWillTopic.isEmpty()) {
            if (lastWillMessage == null) {
                LOG.warn("Last Will Topic is set, but Last Will Message is null. Last Will will not be set.");
            } else {
                // CORRECTED: lastWillQos will never be null due to default value in factory
                int qosToUse = lastWillQos; // Direct assignment, as it's guaranteed to have a value
                boolean retainToUse = (lastWillRetain != null) ? lastWillRetain : false; // Boolean still needs null check for safety

                options.setWill(lastWillTopic, lastWillMessage.getBytes(), qosToUse, retainToUse);
                LOG.info("Configured Last Will: Topic='{}', QoS={}, Retain={}", lastWillTopic, qosToUse, retainToUse);
            }
        }


        options.setAutomaticReconnect(automaticReconnect);
        options.setCleanSession(cleanSession);
        options.setConnectionTimeout(connect_timeout_seconds);
        options.setKeepAliveInterval(keep_alive_internval_seconds);

        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                LOG.error("MQTT Connection lost: {}", cause.getMessage(), cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                messageQueue.put(message.getPayload());
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Not used for a source
            }
        });

        client.connect(options);
        client.subscribe(Stringtopic, qos);
        LOG.info("Connected to MQTT broker and subscribed to topic: {}", Stringtopic);

        deserializer.open(new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return getRuntimeContext().getMetricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return (UserCodeClassLoader) getRuntimeContext().getUserCodeClassLoader();
            }
        });
    }

    /**
     * Helper method to create an SSLSocketFactory.
     * This method contains the core logic for loading keystores/truststores.
     */
    private SSLSocketFactory createSslSocketFactory(
        String truststorePath, String truststorePassword,
        String keystorePath, String keystorePassword, String keyPassword,
        String protocolVersion, String cipherSuites) throws Exception {

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        if (truststorePath != null && !truststorePath.isEmpty()) {
            try (FileInputStream fis = new FileInputStream(truststorePath)) {
                trustStore.load(fis, truststorePassword != null ? truststorePassword.toCharArray() : null);
            }
        } else {
            LOG.warn("No SSL truststore path provided. Trust verification might be limited.");
        }


        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        KeyManagerFactory keyManagerFactory = null;
        if (keystorePath != null && !keystorePath.isEmpty()) {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(keystorePath)) {
                keyStore.load(fis, keystorePassword != null ? keystorePassword.toCharArray() : null);
            }
            keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, (keyPassword != null && !keyPassword.isEmpty()) ? keyPassword.toCharArray() : keystorePassword != null ? keystorePassword.toCharArray() : null);
        }

        SSLContext sslContext = SSLContext.getInstance(protocolVersion != null && !protocolVersion.isEmpty() ? protocolVersion : "TLS");
        sslContext.init(
            (keyManagerFactory != null) ? keyManagerFactory.getKeyManagers() : null,
            trustManagerFactory.getTrustManagers(),
            null
        );

        SSLSocketFactory socketFactory = sslContext.getSocketFactory();

        return socketFactory;
    }


    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (isRunning) {
            try {
                byte[] messageBytes = messageQueue.take();
                if (isRunning) {
                    try {
                        RowData rowData = deserializer.deserialize(messageBytes);
                        if (rowData != null) {
                            ctx.collect(rowData);
                        }
                    } catch (IOException e) {
                        LOG.error("Error deserializing message: {}", e.getMessage(), e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("MQTT message queue was interrupted.", e);
                isRunning = false;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (messageQueue != null) {
            messageQueue.clear();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (client != null && client.isConnected()) {
            try {
                if (client.isConnected() && Stringtopic != null && !Stringtopic.isEmpty()) {
                    client.unsubscribe(Stringtopic);
                    LOG.info("Unsubscribed from MQTT topic: {}", Stringtopic);
                }
                client.disconnect();
                LOG.info("Disconnected from MQTT broker: {}", brokerURL);

            } catch (MqttException e) {
                LOG.error("Error closing MQTT client: {}", e.getMessage(), e);
            }
        }
        if (messageQueue != null) {
            messageQueue.clear();
        }
    }
}