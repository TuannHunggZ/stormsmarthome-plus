package com.storm.iotdata.storm;

import com.storm.iotdata.models.StormConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that joins house average and archive median to compute house forecast values.
 */
public class Bolt_houseForecast extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_houseForecast.class);
	private static final long SECONDS_PER_DAY = 86_400L;
	private static final long SECONDS_PER_MINUTE = 60L;

	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private final String insertSql;
	private final String tableName;
	private final String inputAverageStreamId;
	private final String inputMedianStreamId;
	private final String inputFieldWindowSize;
	private final String inputFieldTimestamp;
	private final String inputFieldHouseId;
	private final String inputFieldCurrentAverage;
	private final String inputFieldArchiveMedian;
	private final long minimumDatasetTimestampSeconds;

	private transient OutputCollector collector;
	private final Map<ForecastKey, ForecastState> forecastStates;
	private transient Connection connection;
	private transient PreparedStatement insertStatement;

	/**
	 * Creates the bolt with configured stream ids and field mappings.
	 */
	public Bolt_houseForecast() {
		this.jdbcUrl = StormConfig.getJdbcUrl();
		this.jdbcUser = StormConfig.getJdbcUser();
		this.jdbcPassword = StormConfig.getJdbcPassword();
		this.tableName = StormConfig.getHouseForecastTableName();
		this.insertSql = String.format(StormConfig.getHouseForecastInsertSql(), tableName);
		this.inputAverageStreamId = "current-house-average";
		this.inputMedianStreamId = "archive-house-median";
		this.inputFieldWindowSize = "windowSize";
		this.inputFieldTimestamp = "timestamp";
		this.inputFieldHouseId = "houseId";
		this.inputFieldCurrentAverage = "currentAverage";
		this.inputFieldArchiveMedian = "archiveMedian";
		this.minimumDatasetTimestampSeconds = StormConfig.getMinimumDatasetTimestampSeconds();
		this.forecastStates = new HashMap<>();
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		initializeDatabase();
	}

	@Override
	public void execute(Tuple input) {
		try {
			String sourceStreamId = input.getSourceStreamId();

			if (inputAverageStreamId.equals(sourceStreamId)) {
				processAverageTuple(input);
			} else if (inputMedianStreamId.equals(sourceStreamId)) {
				processMedianTuple(input);
			} else {
				LOGGER.debug("Ignoring tuple from unexpected stream {}", sourceStreamId);
			}

			collector.ack(input);
		} catch (SQLException exception) {
			rollbackTransaction(exception);
			collector.fail(input);
		} catch (RuntimeException exception) {
			rollbackTransaction(exception);
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No output streams.
	}

	@Override
	public void cleanup() {
		forecastStates.clear();
		closeResources();
	}

	private void processAverageTuple(Tuple input) throws SQLException {
		ForecastKey forecastKey = readForecastKey(input);
		ForecastState forecastState = forecastStates.computeIfAbsent(forecastKey, ignored -> new ForecastState());
		forecastState.average = input.getDoubleByField(inputFieldCurrentAverage);
		forecastState.hasAverage = true;

		if (forecastKey.timestamp < minimumDatasetTimestampSeconds + SECONDS_PER_DAY) {
			persistForecast(forecastKey, forecastState.average);
			forecastStates.remove(forecastKey);
			return;
		}

		if (forecastState.hasMedian) {
			double forecast = (forecastState.average + forecastState.median) / 2.0d;
			persistForecast(forecastKey, forecast);
			forecastStates.remove(forecastKey);
		}
	}

	private void processMedianTuple(Tuple input) throws SQLException {
		ForecastKey forecastKey = readForecastKey(input);
		ForecastState forecastState = forecastStates.computeIfAbsent(forecastKey, ignored -> new ForecastState());
		forecastState.median = input.getDoubleByField(inputFieldArchiveMedian);
		forecastState.hasMedian = true;

		if (forecastState.hasAverage) {
			double forecast = (forecastState.average + forecastState.median) / 2.0d;
			persistForecast(forecastKey, forecast);
			forecastStates.remove(forecastKey);
		}
	}

	private ForecastKey readForecastKey(Tuple input) {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long timestamp = input.getLongByField(inputFieldTimestamp);
		return new ForecastKey(
			windowSize,
			timestamp,
			input.getIntegerByField(inputFieldHouseId)
		);
	}

	private void persistForecast(ForecastKey forecastKey, double forecast) throws SQLException {
		insertStatement.clearParameters();
		insertStatement.setInt(1, forecastKey.windowSize);
		insertStatement.setTimestamp(
			2,
			toSqlTimestamp(forecastKey.timestamp + (2L * forecastKey.windowSize * SECONDS_PER_MINUTE))
		);
		insertStatement.setInt(3, forecastKey.houseId);
		insertStatement.setDouble(4, forecast);
		insertStatement.executeUpdate();
		connection.commit();
	}

	private Timestamp toSqlTimestamp(long epochSeconds) {
		return Timestamp.from(Instant.ofEpochSecond(epochSeconds));
	}

	private void initializeDatabase() {
		try {
			connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
			connection.setAutoCommit(false);
			insertStatement = connection.prepareStatement(insertSql);
			LOGGER.info("Connected to PostgreSQL at {}", jdbcUrl);
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

	private void rollbackTransaction(Exception exception) {
		try {
			if (connection != null) {
				connection.rollback();
				LOGGER.error("Rolled back PostgreSQL transaction due to error", exception);
			}
		} catch (SQLException rollbackException) {
			LOGGER.error("Failed to rollback PostgreSQL transaction", rollbackException);
		}
	}

	private void closeResources() {
		try {
			if (insertStatement != null) {
				insertStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
			LOGGER.info("Closed PostgreSQL resources successfully");
		} catch (SQLException exception) {
			LOGGER.warn("Failed to close PostgreSQL resources cleanly", exception);
		}
	}

	private static final class ForecastKey {

		private final int windowSize;
		private final long timestamp;
		private final int houseId;

		private ForecastKey(int windowSize, long timestamp, int houseId) {
			this.windowSize = windowSize;
			this.timestamp = timestamp;
			this.houseId = houseId;
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (!(other instanceof ForecastKey)) {
				return false;
			}
			ForecastKey forecastKey = (ForecastKey) other;
			return windowSize == forecastKey.windowSize
				&& timestamp == forecastKey.timestamp
				&& houseId == forecastKey.houseId;
		}

		@Override
		public int hashCode() {
			return Objects.hash(windowSize, timestamp, houseId);
		}
	}

	private static final class ForecastState {

		private Double average;
		private Double median;
		private boolean hasAverage;
		private boolean hasMedian;
	}
}
