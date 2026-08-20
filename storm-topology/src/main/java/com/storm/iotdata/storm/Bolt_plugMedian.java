package com.storm.iotdata.storm;

import com.storm.iotdata.models.StormConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that computes historical median load per plug using plug_average.
 */
public class Bolt_plugMedian extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_plugMedian.class);
	private static final int MINUTES_PER_DAY = 1440;
	private static final long SECONDS_PER_MINUTE = 60L;

	private final String inputFieldWindowSize;
	private final String inputFieldTimestamp;
	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private final String selectSqlTemplate;
	private final String inputStreamId;
	private final String outputStreamId;
	private final String outputFieldWindowSize;
	private final String outputFieldTimestamp;
	private final String inputFieldHouseId;
	private final String outputFieldHouseId;
	private final String outputFieldHouseholdId;
	private final String outputFieldPlugId;
	private final String outputFieldArchiveMedian;
	private final long minimumDatasetTimestampSeconds;

	private transient OutputCollector collector;
	private transient Connection connection;
	private transient PreparedStatement selectStatement;

	/**
	 * Creates the bolt with configured SQL and field mappings.
	 */
	public Bolt_plugMedian() {
		this.inputFieldWindowSize = "windowSize";
		this.inputFieldTimestamp = "timestamp";
		this.inputFieldHouseId = "houseId";
		this.jdbcUrl = StormConfig.getJdbcUrl();
		this.jdbcUser = StormConfig.getJdbcUser();
		this.jdbcPassword = StormConfig.getJdbcPassword();
		this.selectSqlTemplate = StormConfig.getPlugMedianSelectByHouseSqlTemplate();
		this.inputStreamId = "median-trigger";
		this.outputStreamId = "archive-plug-median";
		this.outputFieldWindowSize = "windowSize";
		this.outputFieldTimestamp = "timestamp";
		this.outputFieldHouseId = "houseId";
		this.outputFieldHouseholdId = "householdId";
		this.outputFieldPlugId = "plugId";
		this.outputFieldArchiveMedian = "archiveMedian";
		this.minimumDatasetTimestampSeconds = StormConfig.getMinimumDatasetTimestampSeconds();
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		initializeDatabase();
	}

	@Override
	public void execute(Tuple input) {
		try {
			if (inputStreamId.equals(input.getSourceStreamId())) {
				processPunctuation(input);
			} else {
				LOGGER.debug("Ignoring tuple from unexpected stream {}", input.getSourceStreamId());
			}
			collector.ack(input);
		} catch (SQLException exception) {
			collector.fail(input);
		} catch (RuntimeException exception) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(
            outputStreamId,
            new Fields(
                outputFieldWindowSize,
                outputFieldTimestamp,
                outputFieldHouseId,
                outputFieldHouseholdId,
                outputFieldPlugId,
                outputFieldArchiveMedian
            )
        );
	}

	@Override
	public void cleanup() {
		closeResources();
	}

	private void initializeDatabase() {
		try {
			connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
			selectStatement = connection.prepareStatement(String.format(selectSqlTemplate, StormConfig.getPlugAverageTableName()));
			LOGGER.info("Connected to PostgreSQL successfully");
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

	private void processPunctuation(Tuple input) throws SQLException {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long timestamp = input.getLongByField(inputFieldTimestamp);
		int houseId = input.getIntegerByField(inputFieldHouseId);
		long windowSizeSeconds = windowSize * SECONDS_PER_MINUTE;
		long forecastTimestamp = timestamp + (2L * windowSizeSeconds);
		long dayStrideSeconds = calculateSlicesPerDay(windowSize) * windowSizeSeconds;

		LOGGER.info("Forecast timestamp {} for window {}m house {}", forecastTimestamp, windowSize, houseId);

		Map<PlugKey, List<Double>> historicalValues = loadHistoricalValues(windowSize, forecastTimestamp, dayStrideSeconds, houseId);
		int processedPlugCount = emitMedian(windowSize, timestamp, historicalValues);

		LOGGER.info("Number of plugs processed: {}", processedPlugCount);
	}

	private int calculateSlicesPerDay(int windowSize) {
		if (windowSize <= 0) {
			throw new IllegalArgumentException("Window size must be positive: " + windowSize);
		}

		return MINUTES_PER_DAY / windowSize;
	}

	private Map<PlugKey, List<Double>> loadHistoricalValues(int windowSize, long forecastTimestamp, long dayStrideSeconds, int houseId) throws SQLException {
		Map<PlugKey, List<Double>> historicalValues = new HashMap<>();
		int queriedSlices = 0;

		for (long historyTimestamp = forecastTimestamp - dayStrideSeconds; historyTimestamp >= minimumDatasetTimestampSeconds; historyTimestamp -= dayStrideSeconds) {
			loadHistoricalValues(windowSize, historyTimestamp, houseId, historicalValues);
			queriedSlices += 1;
		}

		LOGGER.info("History slices queried: {}", queriedSlices);
		return historicalValues;
	}

	private void loadHistoricalValues(int windowSize, long historyTimestamp, int houseId, Map<PlugKey, List<Double>> historicalValues) throws SQLException {
		selectStatement.clearParameters();
		selectStatement.setInt(1, windowSize);
		selectStatement.setTimestamp(2, toSqlTimestamp(historyTimestamp));
		selectStatement.setInt(3, houseId);

		try (ResultSet resultSet = selectStatement.executeQuery()) {
			while (resultSet.next()) {
				PlugKey key = new PlugKey(
					resultSet.getInt(1),
					resultSet.getInt(2),
					resultSet.getInt(3)
				);

				List<Double> values = historicalValues.computeIfAbsent(key, ignored -> new ArrayList<Double>());
				values.add(resultSet.getDouble(4));
			}
		}
	}

	private int emitMedian(int windowSize, long timestamp, Map<PlugKey, List<Double>> historicalValues) {
		int emittedCount = 0;

		for (Map.Entry<PlugKey, List<Double>> entry : historicalValues.entrySet()) {
			PlugKey plugKey = entry.getKey();
			double archiveMedian = calculateMedian(entry.getValue());
			emitMedian(windowSize, timestamp, plugKey, archiveMedian);
			emittedCount += 1;
		}

		LOGGER.info("Median emitted: {}", emittedCount);
		return emittedCount;
	}

	private double calculateMedian(List<Double> values) {
		if (values == null || values.isEmpty()) {
			return 0.0d;
		}

		List<Double> sortedValues = new ArrayList<Double>(values);
		Collections.sort(sortedValues);

		int size = sortedValues.size();
		int middleIndex = size / 2;

		if ((size & 1) == 0) {
			return (sortedValues.get(middleIndex - 1) + sortedValues.get(middleIndex)) / 2.0d;
		}

		return sortedValues.get(middleIndex);
	}

	private void emitMedian(int windowSize, long timestamp, PlugKey plugKey, double archiveMedian) {
		collector.emit(
			outputStreamId,
			new Values(
				windowSize,
				timestamp,
				plugKey.houseId,
				plugKey.householdId,
				plugKey.plugId,
				archiveMedian
			)
		);
	}

	private void closeResources() {
		try {
			if (selectStatement != null) {
				selectStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
			LOGGER.info("Closed PostgreSQL connection successfully");
		} catch (SQLException exception) {
			LOGGER.warn("Failed to close PostgreSQL resources cleanly", exception);
		}
	}

	private Timestamp toSqlTimestamp(long epochSeconds) {
		return Timestamp.from(Instant.ofEpochSecond(epochSeconds));
	}

	private static final class PlugKey {

		private final int houseId;
		private final int householdId;
		private final int plugId;

		private PlugKey(int houseId, int householdId, int plugId) {
			this.houseId = houseId;
			this.householdId = householdId;
			this.plugId = plugId;
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (!(other instanceof PlugKey)) {
				return false;
			}
			PlugKey plugKey = (PlugKey) other;
			return houseId == plugKey.houseId
				&& householdId == plugKey.householdId
				&& plugId == plugKey.plugId;
		}

		@Override
		public int hashCode() {
			return Objects.hash(houseId, householdId, plugId);
		}
	}
}