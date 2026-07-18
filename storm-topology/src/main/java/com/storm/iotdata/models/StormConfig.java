package com.storm.iotdata.models;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class StormConfig {
    private static final String DEFAULT_CONFIG_RESOURCE = "config/conf.yaml";
    private static final String SP_OUT_DATA_SECTION = "spout-data";
    private static final String DEFAULT_BROKER_URL = "tcp://mqtt-broker:1883";
    private static final String DEFAULT_BROKER_TOPIC = "iotdata";

    private final Map<String, Object> spoutDataConfig;

    public StormConfig() {
        this(loadConfigMap());
    }

    private StormConfig(Map<String, Object> config) {
        this.spoutDataConfig = readSection(config, SP_OUT_DATA_SECTION);
    }

    public SpoutDataConfig getSpoutDataConfig() {
        return new SpoutDataConfig(spoutDataConfig);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadConfigMap() {
        Yaml yaml = new Yaml();

        try (InputStream inputStream = StormConfig.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_RESOURCE)) {
            if (inputStream == null) {
                throw new IllegalStateException("Missing config resource: " + DEFAULT_CONFIG_RESOURCE);
            }

            Object loaded = yaml.load(inputStream);
            if (loaded == null) {
                return Collections.emptyMap();
            }

            if (!(loaded instanceof Map)) {
                throw new IllegalStateException("Invalid config format in " + DEFAULT_CONFIG_RESOURCE);
            }

            return (Map<String, Object>) loaded;
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to load storm config from " + DEFAULT_CONFIG_RESOURCE, exception);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readSection(Map<String, Object> config, String sectionName) {
        Object section = config.get(sectionName);
        if (section == null) {
            return Collections.emptyMap();
        }

        if (!(section instanceof Map)) {
            throw new IllegalStateException("Invalid config section: " + sectionName);
        }

        return (Map<String, Object>) section;
    }

    private static String readString(Map<String, Object> config, String key, String defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }

        String text = value.toString().trim();
        return text.isEmpty() ? defaultValue : text;
    }

    private static int readInt(Map<String, Object> config, String key, int defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException exception) {
            throw new IllegalStateException("Invalid integer config value for key: " + key, exception);
        }
    }

    public static final class SpoutDataConfig {

        private final String brokerUri;
        private final String brokerTopic;
        private final int qos;
        private final int maxEmitPerNextTuple;
        private final int queueCapacity;
        private final String streamIdData;
        private final int propertyLoad;
        private final int connectionTimeoutSeconds;
        private final String fieldId;
        private final String fieldTimestamp;
        private final String fieldValue;
        private final String fieldPlugId;
        private final String fieldHouseholdId;
        private final String fieldHouseId;

        private SpoutDataConfig(Map<String, Object> config) {
            this.brokerUri = readString(config, "broker-uri", DEFAULT_BROKER_URL);
            this.brokerTopic = readString(config, "broker-topic", DEFAULT_BROKER_TOPIC);
            this.qos = readInt(config, "qos", 0);
            this.maxEmitPerNextTuple = readInt(config, "max-emit-per-next-tuple", 100);
            this.queueCapacity = readInt(config, "queue-capacity", 10_000);
            this.streamIdData = readString(config, "stream-id-data", "data");
            this.propertyLoad = readInt(config, "property-load", 1);
            this.connectionTimeoutSeconds = readInt(config, "connection-timeout-seconds", 10);
            this.fieldId = readString(config, "field-id", "id");
            this.fieldTimestamp = readString(config, "field-timestamp", "timestamp");
            this.fieldValue = readString(config, "field-value", "value");
            this.fieldPlugId = readString(config, "field-plug-id", "plugId");
            this.fieldHouseholdId = readString(config, "field-household-id", "householdId");
            this.fieldHouseId = readString(config, "field-house-id", "houseId");
        }

        public String getBrokerUri() {
            return brokerUri;
        }

        public String getBrokerTopic() {
            return brokerTopic;
        }

        public int getQos() {
            return qos;
        }

        public int getMaxEmitPerNextTuple() {
            return maxEmitPerNextTuple;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public String getStreamIdData() {
            return streamIdData;
        }

        public int getPropertyLoad() {
            return propertyLoad;
        }

        public int getConnectionTimeoutSeconds() {
            return connectionTimeoutSeconds;
        }

        public String getFieldId() {
            return fieldId;
        }

        public String getFieldTimestamp() {
            return fieldTimestamp;
        }

        public String getFieldValue() {
            return fieldValue;
        }

        public String getFieldPlugId() {
            return fieldPlugId;
        }

        public String getFieldHouseholdId() {
            return fieldHouseholdId;
        }

        public String getFieldHouseId() {
            return fieldHouseId;
        }
    }
}
