/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Mqtt Source connector
/
/       File            :   MqttTableSourceFactory.java
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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class MqttTableSourceFactory implements DynamicTableSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MqttTableSourceFactory.class);

    public static final String CONNECTOR_ID = "mqtt";

    // Changed BROKER_URL to BROKER_HOST and BROKER_PORT
    public static final ConfigOption<String> BROKER_HOST = ConfigOptions
        .key("broker.host")
        .stringType()
        .noDefaultValue()
        .withDescription("Please provide the MQTT Broker Host");

    public static final ConfigOption<Integer> BROKER_PORT = ConfigOptions
        .key("broker.port")
        .intType()
        .noDefaultValue()
        .withDescription("Please provide the MQTT Broker Port");

    public static final ConfigOption<String> TOPIC = ConfigOptions
        .key("topic")
        .stringType()
        .noDefaultValue()
        .withDescription("Please provide the MQTT Broker Topic");

    public static final ConfigOption<String> FORMAT = ConfigOptions
        .key("format")
        .stringType()
        .defaultValue("json")
        .withDescription("Please provide the Flink Table SQL format [json or ???]");

    public static final ConfigOption<String> CLIENT_ID = ConfigOptions
        .key("client.id")
        .stringType()
        .noDefaultValue()
        .withDescription("Please provide an Client Id to be referenced on the MQTT for the client access");

    public static final ConfigOption<Integer> QOS = ConfigOptions
        .key("qos")
        .intType()
        .defaultValue(1)
        .withDescription("Please provide the MQTT Broker QoS, [0, 1 or 2]");

    public static final ConfigOption<String> USERNAME = ConfigOptions
        .key("username")
        .stringType()
        .noDefaultValue()
        .withDescription("Please provide the MQTT Broker username");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
        .key("password")
        .stringType()
        .noDefaultValue()
        .withDescription("Please provide the MQTT password");

    public static final ConfigOption<Boolean> AUTOMATIC_RECONNECT = ConfigOptions
        .key("automatic-reconnect")
        .booleanType()
        .defaultValue(true)
        .withDescription("Please provide the MQTT Broker configuration for automatic-reconnect [True or False]");

    public static final ConfigOption<Boolean> CLEAN_SESSION = ConfigOptions
        .key("clean-session")
        .booleanType()
        .defaultValue(true)
        .withDescription("Please provide the MQTT Broker configuration for clean-session [True or False]");

    public static final ConfigOption<Boolean> SSL = ConfigOptions
        .key("ssl")
        .booleanType()
        .defaultValue(false)
        .withDescription("Please provide the MQTT Broker configuration for ssl [True or False]");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PATH = ConfigOptions
        .key("ssl.truststore.path")
        .stringType()
        .noDefaultValue()
        .withDescription("Path to the SSL/TLS truststore file.");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD = ConfigOptions
        .key("ssl.truststore.password")
        .stringType()
        .noDefaultValue()
        .withDescription("Password for the SSL/TLS truststore.");

    public static final ConfigOption<String> SSL_KEYSTORE_PATH = ConfigOptions
        .key("ssl.keystore.path")
        .stringType()
        .noDefaultValue()
        .withDescription("Path to the SSL/TLS keystore file (for client authentication).");

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD = ConfigOptions
        .key("ssl.keystore.password")
        .stringType()
        .noDefaultValue()
        .withDescription("Password for the SSL/TLS keystore.");

    public static final ConfigOption<String> SSL_KEY_PASSWORD = ConfigOptions
        .key("ssl.key.password")
        .stringType()
        .noDefaultValue()
        .withDescription("Password for the private key within the SSL/TLS keystore.");

    public static final ConfigOption<String> SSL_PROTOCOL_VERSION = ConfigOptions
        .key("ssl.protocol.version")
        .stringType()
        .defaultValue("TLSv1.2") // Sensible default
        .withDescription("The SSL/TLS protocol version to use (e.g., TLSv1.2, TLSv1.3).");

    public static final ConfigOption<String> SSL_CIPHER_SUITES = ConfigOptions
        .key("ssl.cipher.suites")
        .stringType()
        .noDefaultValue()
        .withDescription("Comma-separated list of SSL/TLS cipher suites.");

    public static final ConfigOption<Integer> CONNECTION_TIMEOUT_SECONDS = ConfigOptions
        .key("connection.timeout")
        .intType()
        .defaultValue(30)
        .withDescription("Sets the maximum time in seconds that the client will wait for the network connection to the MQTT broker to be established.");

    public static final ConfigOption<Integer> KEEP_ALIVE_INTERVAL_SECONDS = ConfigOptions
        .key("keep.alive.interval")
        .intType()
        .defaultValue(60)
        .withDescription("Sets the keep alive interval in seconds. This interval defines the maximum time interval between messages sent or received.");

    public static final ConfigOption<Integer> MAX_RECONNECT_DELAY_SECONDS = ConfigOptions
        .key("max.reconnect.delay")
        .intType()
        .defaultValue(120)
        .withDescription("Sets the maximum delay in seconds before attempting to reconnect to the MQTT broker.");

    public static final ConfigOption<Integer> EXECUTOR_SERVICE_THREADS = ConfigOptions
        .key("executor.service.threads")
        .intType()
        .defaultValue(1)
        .withDescription("Sets the number of threads to use for the MQTT client's executor service.");

    // New Last Will parameters
    public static final ConfigOption<String> LAST_WILL_TOPIC = ConfigOptions
        .key("last.will.topic")
        .stringType()
        .noDefaultValue()
        .withDescription("The topic for the client's last will and testament message.");

    public static final ConfigOption<String> LAST_WILL_MESSAGE = ConfigOptions
        .key("last.will.message")
        .stringType()
        .noDefaultValue()
        .withDescription("The payload for the client's last will and testament message.");

    public static final ConfigOption<Integer> LAST_WILL_QOS = ConfigOptions
        .key("last.will.qos")
        .intType()
        .defaultValue(1)
        .withDescription("The Quality of Service (QoS) for the client's last will and testament message.");

    public static final ConfigOption<Boolean> LAST_WILL_RETAIN = ConfigOptions
        .key("last.will.retain")
        .booleanType()
        .defaultValue(false)
        .withDescription("Whether the client's last will and testament message should be retained.");


    @Override
    public String factoryIdentifier() {
        LOG.info("MqttTableSourceFactory factoryIdentifier called.");
        return CONNECTOR_ID;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Arrays.asList(BROKER_HOST, BROKER_PORT, TOPIC, FORMAT));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(Arrays.asList(
                 FORMAT
                ,QOS
                ,CLIENT_ID
                ,USERNAME
                ,PASSWORD
                ,AUTOMATIC_RECONNECT
                ,CLEAN_SESSION
                ,SSL
                ,SSL_TRUSTSTORE_PATH
                ,SSL_TRUSTSTORE_PASSWORD
                ,SSL_KEYSTORE_PATH
                ,SSL_KEYSTORE_PASSWORD
                ,SSL_KEY_PASSWORD
                ,SSL_PROTOCOL_VERSION
                ,SSL_CIPHER_SUITES
                ,CONNECTION_TIMEOUT_SECONDS
                ,KEEP_ALIVE_INTERVAL_SECONDS
                ,MAX_RECONNECT_DELAY_SECONDS
                ,EXECUTOR_SERVICE_THREADS
                ,LAST_WILL_TOPIC
                ,LAST_WILL_MESSAGE
                ,LAST_WILL_QOS
                ,LAST_WILL_RETAIN));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // Validate all options specified in DDL
        helper.validate();

        ReadableConfig options = helper.getOptions();

        String  brokerHost                      = options.get(BROKER_HOST);
        int     brokerPort                      = options.get(BROKER_PORT);
        boolean ssl                             = options.get(SSL);

        // Construct brokerUrl based on SSL
        String protocol = ssl ? "ssl://" : "tcp://";
        String brokerUrl = protocol + brokerHost + ":" + brokerPort;

        String  topic                           = options.get(TOPIC);
        String  format                          = options.get(FORMAT);
        String  clientId                        = options.get(CLIENT_ID);
        int     qos                             = options.get(QOS);
        String  username                        = options.get(USERNAME);
        String  password                        = options.get(PASSWORD);
        boolean automaticReconnect              = options.get(AUTOMATIC_RECONNECT);
        boolean cleanSession                    = options.get(CLEAN_SESSION);
        int     connect_timeout_seconds         = options.get(CONNECTION_TIMEOUT_SECONDS);
        int     keep_alive_internval_seconds    = options.get(KEEP_ALIVE_INTERVAL_SECONDS);
        int     max_reconnect_delay_seconds     = options.get(MAX_RECONNECT_DELAY_SECONDS);
        int     executor_service_threads        = options.get(EXECUTOR_SERVICE_THREADS);

        // SSL parameters
        String sslTruststorePath = null;
        String sslTruststorePassword = null;
        String sslKeystorePath = null;
        String sslKeystorePassword = null;
        String sslKeyPassword = null;
        String sslProtocolVersion = null;
        String sslCipherSuites = null;

        if (ssl) {
            sslTruststorePath = options.get(SSL_TRUSTSTORE_PATH);
            sslTruststorePassword = options.get(SSL_TRUSTSTORE_PASSWORD);
            sslKeystorePath = options.get(SSL_KEYSTORE_PATH);
            sslKeystorePassword = options.get(SSL_KEYSTORE_PASSWORD);
            sslKeyPassword = options.get(SSL_KEY_PASSWORD);
            sslProtocolVersion = options.get(SSL_PROTOCOL_VERSION);
            sslCipherSuites = options.get(SSL_CIPHER_SUITES);

            if (sslTruststorePath == null || sslTruststorePath.isEmpty()) {
                throw new ValidationException("Option 'ssl.truststore.path' is required when 'ssl' is true.");
            }
        }

        // New Last Will parameters
        String lastWillTopic = options.get(LAST_WILL_TOPIC);
        String lastWillMessage = options.get(LAST_WILL_MESSAGE);
        Integer lastWillQos = options.get(LAST_WILL_QOS); // Use Integer to check if it was set
        Boolean lastWillRetain = options.get(LAST_WILL_RETAIN); // Use Boolean to check if it was set

        // Log the input parameters
        LOG.debug("Creating MQTT Table Source with parameters:");
        LOG.debug("  Broker Url:                   {}", brokerUrl);
        LOG.debug("  Broker Host:                  {}", brokerHost);
        LOG.debug("  Broker Port:                  {}", brokerPort);
        LOG.debug("  Topic:                        {}", topic);
        LOG.debug("  Format:                       {}", format);
        LOG.debug("  Client ID:                    {}", clientId != null && !clientId.isEmpty() ? clientId : "Not specified (Flink will generate)");
        LOG.debug("  QoS:                          {}", qos);
        LOG.debug("  Username:                     {}", username != null && !username.isEmpty() ? username : "Not specified");
        LOG.debug("  Password:                     {}", password != null && !password.isEmpty() ? "********" : "Not specified");
        LOG.debug("  Automatic Reconnect:          {}", automaticReconnect );
        LOG.debug("  Clean Session:                {}", cleanSession );
        LOG.debug("  Ssl:                          {}", ssl );
        if (ssl) {
            LOG.debug("  SSL Truststore Path:          {}", sslTruststorePath);
            LOG.debug("  SSL Keystore Path:            {}", sslKeystorePath);
            LOG.debug("  SSL Protocol Version:         {}", sslProtocolVersion);
            LOG.debug("  SSL Cipher Suites:            {}", sslCipherSuites);
        }
        LOG.debug("  connect_timeout_seconds:      {}", connect_timeout_seconds );
        LOG.debug("  keep_alive_internval_seconds: {}", keep_alive_internval_seconds );
        LOG.debug("  max_reconnect_delay_seconds:  {}", max_reconnect_delay_seconds );
        LOG.debug("  executor_service_threads:     {}", executor_service_threads );
        if (lastWillTopic != null && !lastWillTopic.isEmpty()) {
            LOG.debug("  Last Will Topic:              {}", lastWillTopic);
            LOG.debug("  Last Will Message:            {}", lastWillMessage);
            LOG.debug("  Last Will QoS:                {}", lastWillQos);
            LOG.debug("  Last Will Retain:             {}", lastWillRetain);
        }

        if (LOG.isDebugEnabled()) {
            Map<String, String> properties = new LinkedHashMap<>();

            properties.put("connector", CONNECTOR_ID);
            properties.put(BROKER_HOST.key(), brokerHost);
            properties.put(BROKER_PORT.key(), String.valueOf(brokerPort));
            properties.put(TOPIC.key(), topic);
            properties.put(FORMAT.key(), format);

            if (options.getOptional(QOS).isPresent() || QOS.defaultValue() != qos) {
                 properties.put(QOS.key(), String.valueOf(qos));
            }
            if (options.getOptional(CLIENT_ID).isPresent() || CLIENT_ID.defaultValue() != null && !CLIENT_ID.defaultValue().equals(clientId)) {
                 properties.put(CLIENT_ID.key(), clientId);
            }
            if (options.getOptional(USERNAME).isPresent() || USERNAME.defaultValue() != null && !USERNAME.defaultValue().equals(username)) {
                 properties.put(USERNAME.key(), username);
            }
            if (options.getOptional(PASSWORD).isPresent() || PASSWORD.defaultValue() != null && !PASSWORD.defaultValue().equals(password)) {
                 properties.put(PASSWORD.key(), password);
            }
            if (options.getOptional(AUTOMATIC_RECONNECT).isPresent() || AUTOMATIC_RECONNECT.defaultValue() != automaticReconnect) {
                properties.put(AUTOMATIC_RECONNECT.key(), String.valueOf(automaticReconnect));
            }
            if (options.getOptional(CLEAN_SESSION).isPresent() || CLEAN_SESSION.defaultValue() != cleanSession) {
                properties.put(CLEAN_SESSION.key(), String.valueOf(cleanSession));
            }
            if (options.getOptional(SSL).isPresent() || SSL.defaultValue() != ssl) {
                properties.put(SSL.key(), String.valueOf(ssl));
            }
            if (ssl) {
                if (options.getOptional(SSL_TRUSTSTORE_PATH).isPresent() || SSL_TRUSTSTORE_PATH.defaultValue() != null && !SSL_TRUSTSTORE_PATH.defaultValue().equals(sslTruststorePath)) {
                    properties.put(SSL_TRUSTSTORE_PATH.key(), sslTruststorePath);
                }
                if (options.getOptional(SSL_TRUSTSTORE_PASSWORD).isPresent() || SSL_TRUSTSTORE_PASSWORD.defaultValue() != null && !SSL_TRUSTSTORE_PASSWORD.defaultValue().equals(sslTruststorePassword)) {
                    properties.put(SSL_TRUSTSTORE_PASSWORD.key(), sslTruststorePassword);
                }
                if (options.getOptional(SSL_KEYSTORE_PATH).isPresent() || SSL_KEYSTORE_PATH.defaultValue() != null && !SSL_KEYSTORE_PATH.defaultValue().equals(sslKeystorePath)) {
                    properties.put(SSL_KEYSTORE_PATH.key(), sslKeystorePath);
                }
                if (options.getOptional(SSL_KEYSTORE_PASSWORD).isPresent() || SSL_KEYSTORE_PASSWORD.defaultValue() != null && !SSL_KEYSTORE_PASSWORD.defaultValue().equals(sslKeystorePassword)) {
                    properties.put(SSL_KEYSTORE_PASSWORD.key(), sslKeystorePassword);
                }
                if (options.getOptional(SSL_KEY_PASSWORD).isPresent() || SSL_KEY_PASSWORD.defaultValue() != null && !SSL_KEY_PASSWORD.defaultValue().equals(sslKeyPassword)) {
                    properties.put(SSL_KEY_PASSWORD.key(), sslKeyPassword);
                }
                if (options.getOptional(SSL_PROTOCOL_VERSION).isPresent() || SSL_PROTOCOL_VERSION.defaultValue() != null && !SSL_PROTOCOL_VERSION.defaultValue().equals(sslProtocolVersion)) {
                    properties.put(SSL_PROTOCOL_VERSION.key(), sslProtocolVersion);
                }
                if (options.getOptional(SSL_CIPHER_SUITES).isPresent() || SSL_CIPHER_SUITES.defaultValue() != null && !SSL_CIPHER_SUITES.defaultValue().equals(sslCipherSuites)) {
                    properties.put(SSL_CIPHER_SUITES.key(), sslCipherSuites);
                }
            }
            if (options.getOptional(CONNECTION_TIMEOUT_SECONDS).isPresent() || CONNECTION_TIMEOUT_SECONDS.defaultValue() != connect_timeout_seconds) {
                 properties.put(CONNECTION_TIMEOUT_SECONDS.key(), String.valueOf(connect_timeout_seconds));
            }
            if (options.getOptional(KEEP_ALIVE_INTERVAL_SECONDS).isPresent() || KEEP_ALIVE_INTERVAL_SECONDS.defaultValue() != keep_alive_internval_seconds) {
                 properties.put(KEEP_ALIVE_INTERVAL_SECONDS.key(), String.valueOf(keep_alive_internval_seconds));
            }
            if (options.getOptional(MAX_RECONNECT_DELAY_SECONDS).isPresent() || MAX_RECONNECT_DELAY_SECONDS.defaultValue() != max_reconnect_delay_seconds) {
                 properties.put(MAX_RECONNECT_DELAY_SECONDS.key(), String.valueOf(max_reconnect_delay_seconds));
            }
            if (options.getOptional(EXECUTOR_SERVICE_THREADS).isPresent() || EXECUTOR_SERVICE_THREADS.defaultValue() != executor_service_threads) {
                 properties.put(EXECUTOR_SERVICE_THREADS.key(), String.valueOf(executor_service_threads));
            }
            if (options.getOptional(LAST_WILL_TOPIC).isPresent() || LAST_WILL_TOPIC.defaultValue() != null && !LAST_WILL_TOPIC.defaultValue().equals(lastWillTopic)) {
                properties.put(LAST_WILL_TOPIC.key(), lastWillTopic);
            }
            if (options.getOptional(LAST_WILL_MESSAGE).isPresent() || LAST_WILL_MESSAGE.defaultValue() != null && !LAST_WILL_MESSAGE.defaultValue().equals(lastWillMessage)) {
                properties.put(LAST_WILL_MESSAGE.key(), lastWillMessage);
            }
            if (options.getOptional(LAST_WILL_QOS).isPresent() || LAST_WILL_QOS.defaultValue() != lastWillQos) {
                properties.put(LAST_WILL_QOS.key(), String.valueOf(lastWillQos));
            }
            if (options.getOptional(LAST_WILL_RETAIN).isPresent() || LAST_WILL_RETAIN.defaultValue() != lastWillRetain) {
                properties.put(LAST_WILL_RETAIN.key(), String.valueOf(lastWillRetain));
            }

            String propertiesString = properties.entrySet().stream()
                    .map(entry -> String.format("'%s' = '%s'", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining(",\n    "));

            LOG.debug("Reconstructed CREATE TABLE WITH properties:\nWITH (\n    {}\n)", propertiesString);
        }

        DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT
        );

        return new MqttTableSource(
                 brokerUrl
                ,topic
                ,format
                ,clientId
                ,qos
                ,username
                ,password
                ,automaticReconnect
                ,cleanSession
                ,ssl
                ,connect_timeout_seconds
                ,keep_alive_internval_seconds
                ,max_reconnect_delay_seconds
                ,executor_service_threads
                ,producedDataType
                ,decodingFormat
                ,sslTruststorePath
                ,sslTruststorePassword
                ,sslKeystorePath
                ,sslKeystorePassword
                ,sslKeyPassword
                ,sslProtocolVersion
                ,sslCipherSuites
                ,lastWillTopic
                ,lastWillMessage
                ,lastWillQos
                ,lastWillRetain
        );
    }
}