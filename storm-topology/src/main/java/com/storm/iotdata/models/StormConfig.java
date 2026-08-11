package com.storm.iotdata.models;

import java.util.Arrays;
import java.util.List;

public class StormConfig {

    // =====================================================================
    // TIME SLICES (Punctuation Generation)
    // =====================================================================
    // Window sizes, in minutes, used to generate punctuation events
    // across the topology. Each value defines a separate punctuation stream.
    private static final List<Integer> timeSliceMinutes = Arrays.asList(1, 5, 10, 15, 20, 30, 60, 120);

    public static List<Integer> getTimeSliceMinutes() {
        return timeSliceMinutes;
    }

    // =====================================================================
    // DATABASE CONFIGURATION
    // =====================================================================
    // JDBC URL for PostgreSQL.
    private static final String jdbcUrl = "jdbc:postgresql://timescaledb:5432/iotdata";

    // PostgreSQL username.
    private static final String jdbcUser = "postgres";

    // PostgreSQL password.
    private static final String jdbcPassword = "postgres";

    // Target table for plug averages.
    private static final String plugAverageTableName = "plug_average";

    // Target table for house averages.
    private static final String houseAverageTableName = "house_average";

    // Target table for plug forecasts.
    private static final String plugForecastTableName = "plug_forecast";

    // Target table for house forecasts.
    private static final String houseForecastTableName = "house_forecast";

    // Lower bound of the dataset in unix timestamp seconds.
    private static final long minimumDatasetTimestampSeconds = 1377986401L;

    public static String getJdbcUrl() {
        return jdbcUrl;
    }

    public static String getJdbcUser() {
        return jdbcUser;
    }

    public static String getJdbcPassword() {
        return jdbcPassword;
    }

    public static String getPlugAverageTableName() {
        return plugAverageTableName;
    }

    public static String getHouseAverageTableName() {
        return houseAverageTableName;
    }

    public static String getPlugForecastTableName() {
        return plugForecastTableName;
    }

    public static String getHouseForecastTableName() {
        return houseForecastTableName;
    }

    public static long getMinimumDatasetTimestampSeconds() {
        return minimumDatasetTimestampSeconds;
    }

    // =====================================================================
    // ANOMALY DETECTION CONFIGURATION
    // =====================================================================
    // Threshold, in percent, used by anomaly detection bolts.
    private static final int anomalyThresholdPercent = 20;

    // Flags to enable/disable anomaly detection checks.
    private static final boolean plugCheckMax = true;
    private static final boolean plugCheckAvg = true;
    private static final boolean plugCheckMin = true;

    public static int getAnomalyThresholdPercent() {
        return anomalyThresholdPercent;
    }

    public static boolean isPlugCheckMax() {
        return plugCheckMax;
    }

    public static boolean isPlugCheckAvg() {
        return plugCheckAvg;
    }

    public static boolean isPlugCheckMin() {
        return plugCheckMin;
    }

    // =====================================================================
    // SPOUT-DATA
    // =====================================================================
    // MQTT broker URI that the spout connects to.
    private static final String brokerUri = "tcp://mqtt-broker:1883";

    // MQTT topic consumed by the spout.
    private static final String brokerTopic = "iot-data";

    // MQTT subscription QoS used by the spout.
    private static final int qos = 0;

    // Maximum number of stream events emitted by one nextTuple() call.
    private static final int maxEmitPerNextTuple = 100;

    // Maximum number of stream events buffered before new messages are dropped.
    private static final int queueCapacity = 10000;

    // Storm stream id used for data tuples.
    private static final String streamIdData = "data";

    // MQTT property value that identifies a load event.
    private static final int propertyLoad = 1;

    // MQTT connection timeout in seconds.
    private static final int connectionTimeoutSeconds = 10;

    public static String getBrokerUri() {
        return brokerUri;
    }

    public static String getBrokerTopic() {
        return brokerTopic;
    }

    public static int getQos() {
        return qos;
    }

    public static int getMaxEmitPerNextTuple() {
        return maxEmitPerNextTuple;
    }

    public static int getQueueCapacity() {
        return queueCapacity;
    }

    public static String getStreamIdData() {
        return streamIdData;
    }

    public static int getPropertyLoad() {
        return propertyLoad;
    }

    public static int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    // =====================================================================
    // BOLT-AVERAGE-PERSISTENCE
    // =====================================================================
    // Number of records to buffer before flushing a batch.
    private static final int batchSize = 1000;

    // SQL template for plug inserts.
    private static final String plugAverageInsertSql = "INSERT INTO %s (window_size, timestamp, house_id, household_id, plug_id, average_load) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";

    // SQL template for house inserts.
    private static final String houseAverageInsertSql = "INSERT INTO %s (window_size, timestamp, house_id, average_load) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING";

    public static int getBatchSize() {
        return batchSize;
    }

    public static String getPlugAverageInsertSql() {
        return plugAverageInsertSql;
    }

    public static String getHouseAverageInsertSql() {
        return houseAverageInsertSql;
    }

    // =====================================================================
    // BOLT-PLUG-MEDIAN
    // =====================================================================
    // SQL template used to query historical plug averages.
    private static final String plugMedianSelectSqlTemplate = "SELECT house_id, household_id, plug_id, average_load FROM %s WHERE window_size = ? AND timestamp = ?";

    public static String getPlugMedianSelectSqlTemplate() {
        return plugMedianSelectSqlTemplate;
    }

    // =====================================================================
    // BOLT-HOUSE-MEDIAN
    // =====================================================================
    // SQL template used to query historical house averages.
    private static final String houseMedianSelectSqlTemplate = "SELECT house_id, average_load FROM %s WHERE window_size = ? AND timestamp = ?";

    public static String getHouseMedianSelectSqlTemplate() {
        return houseMedianSelectSqlTemplate;
    }

    // =====================================================================
    // BOLT-PLUG-FORECAST
    // =====================================================================
    // SQL template for plug forecast inserts.
    private static final String plugForecastInsertSql = "INSERT INTO %s (window_size, timestamp, house_id, household_id, plug_id, forecast_load) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";

    public static String getPlugForecastInsertSql() {
        return plugForecastInsertSql;
    }

    // =====================================================================
    // BOLT-HOUSE-FORECAST
    // =====================================================================
    // SQL template for house forecast inserts.
    private static final String houseForecastInsertSql = "INSERT INTO %s (window_size, timestamp, house_id, forecast_load) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING";

    public static String getHouseForecastInsertSql() {
        return houseForecastInsertSql;
    }
}