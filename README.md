## Build MVP MQTT Flink Source connector

Ok, as per my normal Blog postings... Rabbit Holes... 

This all started as a simply MVP for a [Apache Flink](https://flink.apache.org) [Mqtt](https://mqtt.org) Source Connector. 

This was mostly done to demostrate a IoT pipeline based on a source of json payload being written [Mqtt](https://mqtt.org) broker, consumed via this source connector, consumed by flink and pushed into Fluss and into [Prometheus](https://prometheus.io) with a [Grafana Dashboard](https://grafana.com).

As part of this the [Eclipse library](https://github.com/eclipse-paho/paho.mqtt.java/tree/master) used is based around org.eclipse.paho.client.mqttv3. 


GIT REPO: []()


In `<root>` execute `make build`

Now copy the 

- `<root>/mqtt-source/target/mqtt*.jar` & 
- `<root>/mqtt-job/target/mqtt*.jar` files to Apache Flink lib directory, in my case I Docker Compose mounted a local volume into
- `<root>/devlab0/conf/flink/lib/flink`


## NOTE: 

The Maven Compile builds for [Apache Flink](https://hub.docker.com/_/flink) [1.20.1](docker pull flink:1.20.1-scala_2.12-java17) and Java 17.

### Create catalog and database

```SQL
USE CATALOG default_catalog;

CREATE CATALOG hive_catalog WITH (
'type'          = 'hive',
'hive-conf-dir' = './conf/'
);

USE CATALOG hive_catalog;

create database mqtt;

show databases;
```

### Flink Sql Create Table

```SQL
CREATE OR REPLACE TABLE hive_catalog.mqtt.factory_iot_101 (
    ts               BIGINT,
    metadata         ROW<
        siteId        INTEGER
        ,deviceId      INTEGER
        ,sensorId      INTEGER
        ,unit          STRING
        ,ts_human      STRING
        ,location      ROW<
            latitude    DOUBLE
            ,longitude   DOUBLE>
        ,deviceType    STRING>
    ,measurement     DOUBLE
) WITH (
     'connector'                    = 'mqtt'
    ,'broker.host'                  = 'localhost'           -- Example: your MQTT broker's hostname or IP
    ,'broker.port'                  = '1883'                -- Example: your MQTT broker's port (1883 for non-SSL)
    ,'topic'                        = 'factory_iot/north/101'
    ,'format'                       = 'json'                -- See https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/overview/
    ,'client.id'                    = 'flink_iot_consumer'
    ,'qos'                          = '1'                   -- Valid integer value (0, 1, or 2)
    ,'username'                     = 'your_mqtt_user'      -- Replace with actual username
    ,'password'                     = 'your_mqtt_pass'      -- Replace with actual password
    ,'automatic-reconnect'          = 'true'                -- Valid boolean value ('true' or 'false')
    ,'clean-session'                = 'true'                -- Valid boolean value ('true' or 'false')
    ,'ssl'                          = 'false'               -- Set to 'true' if using SSL and configure below
    -- SSL Configuration (uncomment and configure if 'ssl' is 'true')
    -- ,'ssl.truststore.path'          = '/path/to/your/truststore.jks'
    -- ,'ssl.truststore.password'      = 'truststore_password'
    -- ,'ssl.keystore.path'            = '/path/to/your/keystore.jks'
    -- ,'ssl.keystore.password'        = 'keystore_password'
    -- ,'ssl.key.password'             = 'key_password'
    -- ,'ssl.protocol.version'         = 'TLSv1.2'
    -- ,'ssl.cipher.suites'            = 'TLS_AES_256_GCM_SHA384' -- Example, specify required suites
    ,'connection.timeout'           = '30'
    ,'keep.alive.interval'          = '60'
    ,'max.reconnect.delay'          = '120'
    ,'executor.service.threads'     = '1'
    ,'last.will.topic'              = 'disconnected/client'
    ,'last.will.qos'                = '1'     
    ,'last.will.retain'             = 'true'              
);
```


## Example MQTT payload.

The below are 3 Example Test json payloads that was uploaded to the topic and then consumed, resulting in working select statement.

### Basic min IoT Payload, produced by Factories 101 and 104

```json5
{
    "ts": 1729312551000, 
    "metadata": {
        "siteId": 101, 
        "deviceId": 1004, 
        "sensorId": 10034, 
        "unit": "BAR"
    }, 
    "measurement": 120
}
```

### Basic min IoT Payload, with a human readable time stamp & location added, produced by Factories 102 and 105

```json5
{
    "ts": 1713807946000, 
    "metadata": {
        "siteId": 102, 
        "deviceId": 1008, 
        "sensorId": 10073, 
        "unit": "Liter", 
        "ts_human": "2024-04-22T19:45:46.000000", 
        "location": {
            "latitude": -33.924869, 
            "longitude": 18.424055
        }
    }, 
    "measurement": 25
}
```

### Complete IoT Payload, with deviceType tag added, produced by Factories 103 and 106

```json5
{
    "ts": 1707882120000, 
    "metadata": {
        "siteId": 103, 
        "deviceId": 1014, 
        "sensorId": 10124, 
        "unit": "Amp", 
        "ts_human": "2024-02-14T05:42:00.000000", 
        "location": {
            "latitude": -33.9137, 
            "longitude": 25.5827
        }, 
        "deviceType": "Hoist_Motor"
    },
    "measurement": 24
}
```


## Example Select statement.

In your sql-client inside your Jobmanager execute:

`use hive_catalog.mqtt;`


```SQL
select ts, metadata.siteId, metadata.deviceId, metadata.sensorId, metadata.unit, metadata.ts_human, metadata.location.latitude, metadata.location.latitude, measurement, ts_WM from factory_iot_103;
```


## High Level overview of the Apache Flink MQTT Source Connector

### Code Flow - Create table

When a table is created using Flink SQL and subsequently used in an INSERT INTO statement, your project's functions and files play specific roles within Flink's execution order:

1. SQL Parsing and Validation (Flink Internal + `MqttTableSourceFactory.java`):

2. When you execute a CREATE TABLE DDL statement for your MQTT source (e.g., `CREATE TABLE mqtt_source_table ...  WITH ('connector' = 'mqtt', ...))`, Flink's internal SQL parser processes the statement.

During the validation phase, Flink recognizes the `'connector' = 'mqtt'` property. 

3. This triggers the lookup for a `DynamicTableSourceFactory` with the identifier `"mqtt"`. This is where your `MqttTableSourceFactory` comes into play.

4. Flink calls methods within `MqttTableSourceFactory` (like `requiredOptions()` and `optionalOptions()`) to validate the provided table properties against what your connector expects. If there are missing or invalid options, `ValidationExceptions` would be thrown here (as we saw and corrected earlier).

Logical Plan Generation (Flink Internal):

5. Flink generates an abstract logical plan for the CREATE TABLE statement based on the validated definition. This is a conceptual representation of your MQTT source table.
Optimization (Flink Internal):

6. The logical plan is optimized by Flink's query optimizer. For a CREATE TABLE statement, this mostly involves ensuring the metadata is correctly structured. Physical Plan Generation and Table Registration (DDL - `MqttTableSourceFactory.java` & `MqttTableSource.java`):

7. Flink's Table API layer then calls the `createDynamicTableSource()` method in your `MqttTableSourceFactory.java`.
Inside this method, `MqttTableSourceFactory` creates and returns an instance of your `MqttTableSource`.java class. This `MqttTableSource` object encapsulates the configuration of your MQTT connector `(broker URL, topic, QoS, client ID, username, password, auto-reconnect, clean session, and deserialization format)`.

8. The `MqttTableSource` is then registered in Flink's catalog under the name you specified in your CREATE TABLE statement (e.g., `mqtt_source_table`). At this point, Flink knows how to interact with an MQTT source based on your connector.

9.  Execution (DML - `MqttJob.java`, `MqttTableSource.java`, `MqttSourceFunction.java`):

10.  When your `MqttJob.java` runs and executes the `INSERT INTO my_print_sink SELECT ... FROM mqtt_source_table` SQL statement, the actual data flow begins:

   1. `MqttJob.java`: This is the entry point. It sets up the `StreamExecutionEnvironment`, `StreamTableEnvironment`, retrieves the source table schema dynamically, constructs the `INSERT INTO SQL`, and then calls `tableEnv.executeSql()`.
   
   2. `MqttTableSource`.java: Flink asks the `mqtt_source_table` (which is an instance of `MqttTableSource`) for its runtime execution provider. This triggers a call to `MqttTableSource.getScanRuntimeProvider()`.
   
   3. Inside `getScanRuntimeProvider()`, your `MqttTableSource` uses the `DecodingFormat` to create a `DeserializationSchema` (e.g., for JSON parsing). Crucially, it then instantiates and returns a `SourceFunctionProvider` that wraps your `MqttSourceFunction.java`.
   
   4. `MqttSourceFunction.java`: This is the core component that performs the actual data ingestion:
   
   5. Its open() method is called when the Flink Task Manager starts the task. This is where the MQTT client (`org.eclipse.paho.client.mqttv3.MqttClient`) connects to the broker, sets up connection options (including `automaticReconnect` and `cleanSession` values you configured), and subscribes to the MQTT topic.
   
   6. The `run()` method continuously listens for messages from the MQTT topic, puts raw byte messages into an internal queue, and then uses the `DeserializationSchema` to parse them into Flink's `RowData` format. It then `collect()`s these `RowData` records, making them available to the downstream Flink pipeline.
   
   7. The cancel() and close() methods handle the graceful shutdown of the MQTT client when the Flink job stops or is cancelled.

11.  In summary, the `MqttTableSourceFactory` and `MqttTableSource` define how your MQTT connector integrates with Flink's Table API, while `MqttSourceFunction` contains the runtime logic for actually consuming data from MQTT, all orchestrated by the `MqttJob` and Flink's internal SQL engine.

12.  Once the above was made to work I expanded the base classes to included as many of the eclipse MQTT client variables.
    
    
### Code Flow - Record Publish on MQTT Topic

When records are published on the MQTT topic, here's the order of execution of your project's functions and the internal loops involved to expose those records in your created Flink table:

MQTT Message Publication:

1. An external client publishes a message to the MQTT topic that your Flink job is subscribed to.
`MqttCallback.messageArrived()` (`MqttSourceFunction.java`):

2. The `org.eclipse.paho.client.mqttv3.MqttClient` (initialized and connected in `MqttSourceFunction.open()`) receives the incoming MQTT message.

3. This triggers the `messageArrived(String topic, MqttMessage message)` callback method within the anonymous MqttCallback implementation inside your `MqttSourceFunction.java`.
messageQueue.put() (`MqttSourceFunction.java`):

4. Inside the `messageArrived()` method, the raw byte payload of the received MQTT message (message.getPayload()) is immediately added to the LinkedBlockingQueue named messageQueue. This method quickly enqueues the message, allowing the MQTT client thread to process subsequent incoming messages without delay.
`MqttSourceFunction.run()` loop (`MqttSourceFunction.java`):

5. The `run(SourceContext<RowData> ctx)` method of your `MqttSourceFunction` is executed by a Flink task. This method contains an infinite while (isRunning) loop. This loop is the heart of the source function, continuously pulling data.
messageQueue.take() (`MqttSourceFunction.java`):

6. Inside the `run()` loop, `messageQueue.take()` is called. This is a blocking operation. The Flink source task will pause at this line until a message becomes available in the messageQueue (i.e., until `messageArrived()` puts a message into it). Once a message is available, `take()` retrieves it.
`deserializer.deserialize()` (`MqttSourceFunction.java`):

7. The retrieved raw message bytes (messageBytes) are then passed to the `deserializer.deserialize(messageBytes)` method.
This deserializer (which is an instance of `DeserializationSchema<RowData>`, created based on your table's FORMAT property, e.g., JSON) is responsible for parsing the raw bytes into Flink's internal structured `RowData` format according to the schema you defined in your Flink SQL CREATE TABLE statement. Error handling for malformed messages also typically occurs here.
`ctx.collect()` (`MqttSourceFunction.java`):

8. If the `deserializer.deserialize()` method successfully produces a `RowData` object (i.e., it's not null), `ctx.collect(rowData)` is called.

9. This collect() method emits the `RowData` record into the Flink streaming data pipeline. From this point onwards, the record is available for subsequent transformations, filtering, and ultimately, insertion into your `my_print_sink` table as defined by your `MqttJob.java`.

10. This continuous loop in `MqttSourceFunction.run()`, in conjunction with the `messageArrived` callback and the `LinkedBlockingQueue`, ensures that every record published to the MQTT topic is efficiently captured, deserialized, and exposed as a Flink record in your table.

## Notes

- [Eclipse MQTT](https://eclipse.dev/paho/files/javadoc/index.html)

- [User-defined Sources & Sinks](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sourcessinks/)

- [Implementing a Custom Source Connector for Table API and SQL - Part 1](https://flink.apache.org/2021/09/07/implementing-a-custom-source-connector-for-table-api-and-sql-part-one/)

- [Implementing a custom source connector for Table API and SQL - Part 2](https://flink.apache.org/2021/09/07/implementing-a-custom-source-connector-for-table-api-and-sql-part-two/)

- [DataStream Connectors](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/overview/)

## Credits

As I at best hack when I code, does not matter the language, (it's not my day job), and even less so with Java... I took this on (not sure what I expected)...

This connector would in no way have been possible without allot of questions to and then patient answers by Dian Fu, Thank you.

    Dian Fu
        Senior Technical Specialist at Alibaba Cloud
        https://www.linkedin.com/in/dian-fu-07797493/


### By:

George

[georgelza@gmail.com](georgelza@gmail.com)

[George on Linkedin](https://www.linkedin.com/in/george-leonard-945b502/)

[George on Medium](https://medium.com/@georgelza)

