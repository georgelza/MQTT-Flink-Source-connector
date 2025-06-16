/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink Mqtt Source connector
/
/       File            :   MqttDeserializationSchema.java
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;

import java.io.IOException;
import java.util.Map;

public class MqttDeserializationSchema implements DeserializationSchema<Map<String, Object>> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Object> deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, Map.class);
    }

    @Override
    public boolean isEndOfStream(Map<String, Object> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Map<String, Object>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, Object>>(){});
    }
}