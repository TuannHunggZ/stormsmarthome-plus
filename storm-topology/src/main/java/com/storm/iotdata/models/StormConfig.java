package com.storm.iotdata.models;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StormConfig {
    private static final String DEFAULT_CONFIG_RESOURCE = "config/conf.yaml";
    private static final String TIME_SLICES_KEY = "timeslices";
    private static final String SPOUT_DATA_SECTION = "spout-data";
    private static final String BOLT_CURRENT_PLUG_AVERAGE_SECTION = "bolt-current-plug-average";

    private final List<Integer> timeSlicesMinutes;
    private final SpoutDataConfig spoutDataConfig;
    private final BoltCurrentPlugAverageConfig boltCurrentPlugAverageConfig;

    public StormConfig() {
        this(loadConfigMap());
    }

    private StormConfig(Map<String, Object> config) {
        this.timeSlicesMinutes = readIntegerList(config, TIME_SLICES_KEY);
        this.spoutDataConfig = new SpoutDataConfig(
            readSection(config, SPOUT_DATA_SECTION)
        );
        this.boltCurrentPlugAverageConfig = new BoltCurrentPlugAverageConfig(
            readSection(config, BOLT_CURRENT_PLUG_AVERAGE_SECTION)
        );
    }

    public SpoutDataConfig getSpoutDataConfig() {
        return spoutDataConfig;
    }

    public List<Integer> getTimeSlicesMinutes() {
        return timeSlicesMinutes;
    }

    public BoltCurrentPlugAverageConfig getBoltCurrentPlugAverageConfig() {
        return boltCurrentPlugAverageConfig;
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

    private static List<Integer> readIntegerList(Map<String, Object> config, String key) {
        Object value = config.get(key);
        if (value == null) {
            throw new IllegalStateException("Missing config list: " + key);
        }

        if (!(value instanceof Iterable)) {
            throw new IllegalStateException("Invalid config list for key: " + key);
        }

        List<Integer> result = new ArrayList<>();
        for (Object element : (Iterable<?>) value) {
            try {
                int parsed = Integer.parseInt(element.toString().trim());
                if (parsed <= 0) {
                    throw new IllegalStateException("Config list values must be positive: " + key);
                }
                result.add(parsed);
            } catch (NumberFormatException exception) {
                throw new IllegalStateException("Invalid integer in config list: " + key, exception);
            }
        }

        if (result.isEmpty()) {
            throw new IllegalStateException("Config list must not be empty: " + key);
        }

        Collections.sort(result);

        List<Integer> deduplicated = new ArrayList<>();
        Integer previous = null;
        for (Integer slice : result) {
            if (!slice.equals(previous)) {
                deduplicated.add(slice);
                previous = slice;
            }
        }

        return Collections.unmodifiableList(deduplicated);
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
        private final String fieldWindowSize;
        private final String fieldSliceIndex;

        private SpoutDataConfig(Map<String, Object> config) {
            this.brokerUri = readString(config, "broker-uri", "tcp://mqtt-broker:1883");
            this.brokerTopic = readString(config, "broker-topic", "iotdata");
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
            this.fieldWindowSize = readString(config, "field-window-size", "windowSize");
            this.fieldSliceIndex = readString(config, "field-slice-index", "sliceIndex");
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

        public String getFieldWindowSize() {
            return fieldWindowSize;
        }

        public String getFieldSliceIndex() {
            return fieldSliceIndex;
        }
    }

    public static final class BoltCurrentPlugAverageConfig {

        private final String inputStreamData;
        private final String inputFieldValue;
        private final String inputFieldPlugId;
        private final String inputFieldHouseholdId;
        private final String inputFieldHouseId;
        private final String inputFieldWindowSize;
        private final String inputFieldSliceIndex;
        private final String outputFieldWindowSize;
        private final String outputFieldSliceIndex;
        private final String outputFieldHouseId;
        private final String outputFieldHouseholdId;
        private final String outputFieldPlugId;
        private final String outputFieldCurrentAverage;

        private BoltCurrentPlugAverageConfig(Map<String, Object> config) {
            this.inputStreamData = readString(config, "input-stream-data", "data");
            this.inputFieldValue = readString(config, "input-field-value", "value");
            this.inputFieldPlugId = readString(config, "input-field-plug-id", "plugId");
            this.inputFieldHouseholdId = readString(config, "input-field-household-id", "householdId");
            this.inputFieldHouseId = readString(config, "input-field-house-id", "houseId");
            this.inputFieldWindowSize = readString(config, "input-field-window-size", "windowSize");
            this.inputFieldSliceIndex = readString(config, "input-field-slice-index", "sliceIndex");
            this.outputFieldWindowSize = readString(config, "output-field-window-size", "windowSize");
            this.outputFieldSliceIndex = readString(config, "output-field-slice-index", "sliceIndex");
            this.outputFieldHouseId = readString(config, "output-field-house-id", "houseId");
            this.outputFieldHouseholdId = readString(config, "output-field-household-id", "householdId");
            this.outputFieldPlugId = readString(config, "output-field-plug-id", "plugId");
            this.outputFieldCurrentAverage = readString(config, "output-field-current-average", "currentAverage");
        }

        public String getInputStreamData() {
            return inputStreamData;
        }

        public String getInputFieldValue() {
            return inputFieldValue;
        }

        public String getInputFieldPlugId() {
            return inputFieldPlugId;
        }

        public String getInputFieldHouseholdId() {
            return inputFieldHouseholdId;
        }

        public String getInputFieldHouseId() {
            return inputFieldHouseId;
        }

        public String getInputFieldWindowSize() {
            return inputFieldWindowSize;
        }

        public String getInputFieldSliceIndex() {
            return inputFieldSliceIndex;
        }

        public String getOutputFieldWindowSize() {
            return outputFieldWindowSize;
        }

        public String getOutputFieldSliceIndex() {
            return outputFieldSliceIndex;
        }

        public String getOutputFieldHouseId() {
            return outputFieldHouseId;
        }

        public String getOutputFieldHouseholdId() {
            return outputFieldHouseholdId;
        }

        public String getOutputFieldPlugId() {
            return outputFieldPlugId;
        }

        public String getOutputFieldCurrentAverage() {
            return outputFieldCurrentAverage;
        }
    }
}