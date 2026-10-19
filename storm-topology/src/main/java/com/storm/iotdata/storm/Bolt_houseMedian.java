package com.storm.iotdata.storm;

import com.storm.iotdata.models.StormConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Bolt that computes historical median load per house using house_average.
 */
public class Bolt_houseMedian extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_houseMedian.class);
	private static final long SECONDS_PER_MINUTE = 60L;

	private final String inputFieldWindowSize;
	private final String inputFieldTimestamp;
	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private final String selectSqlTemplate;
	private final String inputPersistenceStreamId;
	private final String inputForecastStreamId;

	private transient OutputCollector collector;
	private transient Connection connection;
	private transient PreparedStatement selectStatement;
	private transient JedisPool redisPool;
	private final Map<CompletionKey, CompletionState> completionStates;

	/**
	 * Creates the bolt with configured SQL and field mappings.
	 *
	 * @param config Shared bolt configuration.
	 */
	public Bolt_houseMedian() {
		this.inputFieldWindowSize = "windowSize";
		this.inputFieldTimestamp = "timestamp";
		this.jdbcUrl = StormConfig.getJdbcUrl();
		this.jdbcUser = StormConfig.getJdbcUser();
		this.jdbcPassword = StormConfig.getJdbcPassword();
		this.selectSqlTemplate = StormConfig.getHouseMedianSelectSqlTemplate();
		this.inputPersistenceStreamId = "house-average-persisted";
		this.inputForecastStreamId = "house-forecast-persisted";
		this.completionStates = new HashMap<>();
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		initializeDatabase();
		redisPool = new JedisPool(
			new JedisPoolConfig(),
			StormConfig.getRedisHost(),
			StormConfig.getRedisPort()
		);
	}

	@Override
	public void execute(Tuple input) {
		try {
			String sourceStreamId = input.getSourceStreamId();
			if (inputPersistenceStreamId.equals(sourceStreamId)) {
				processCompletion(input, true);
			} else if (inputForecastStreamId.equals(sourceStreamId)) {
				processCompletion(input, false);
			} else {
				LOGGER.debug("Ignoring tuple from unexpected stream {}", sourceStreamId);
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
		// Cache updater, no output streams.
	}

	@Override
	public void cleanup() {
		completionStates.clear();
		closeResources();
	}

	private void initializeDatabase() {
		try {
			connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
			selectStatement = connection.prepareStatement(String.format(selectSqlTemplate, "house_average"));
			LOGGER.info("Connected to PostgreSQL successfully");
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

	private void processCompletion(Tuple input, boolean persistenceCompleted) throws SQLException {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long timestamp = input.getLongByField(inputFieldTimestamp);
		CompletionKey completionKey = new CompletionKey(windowSize, timestamp);
		CompletionState completionState = completionStates.computeIfAbsent(
			completionKey,
			ignored -> new CompletionState()
		);

		if (persistenceCompleted) {
			completionState.persistenceCompleted = true;
		} else {
			completionState.forecastCompleted = true;
		}

		if (!completionState.persistenceCompleted || !completionState.forecastCompleted) {
			return;
		}

		completionStates.remove(completionKey);
		processMedian(windowSize, timestamp);
	}

	private void processMedian(int windowSize, long timestamp) throws SQLException {
		long windowSizeSeconds = windowSize * SECONDS_PER_MINUTE;
		int sliceIndexInDay = calculateSliceIndexInDay(timestamp, windowSizeSeconds);

		LOGGER.info("Updating median cache for slice {} and window {}m", sliceIndexInDay, windowSize);
		int processedHouseCount = updateMedianCache(windowSize, sliceIndexInDay, timestamp);

		LOGGER.info("Updated median cache for {} houses", processedHouseCount);
	}

	private int calculateSliceIndexInDay(long timestamp, long windowSizeSeconds) {
		return (int) Math.floorDiv(timestamp % (86400L), windowSizeSeconds);
	}

	private int updateMedianCache(int windowSize, int sliceIndexInDay, long timestamp) throws SQLException {
		selectStatement.clearParameters();
		selectStatement.setInt(1, windowSize);
		selectStatement.setInt(2, sliceIndexInDay);

		int updatedCount = 0;
		try (ResultSet resultSet = selectStatement.executeQuery();
			Jedis jedis = redisPool.getResource()) {
			Pipeline pipeline = jedis.pipelined();
			while (resultSet.next()) {
				int houseId = resultSet.getInt(1);
				String key = String.format(
					"median:house:%d:%d:%d",
					windowSize,
					sliceIndexInDay,
					houseId
				);
				pipeline.set(key, Double.toString(resultSet.getDouble(2)));
				updatedCount += 1;
			}
			pipeline.sync();
		}
		return updatedCount;
	}

	private void closeResources() {
		try {
			if (selectStatement != null) {
				selectStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
			if (redisPool != null) {
				redisPool.close();
			}
			LOGGER.info("Closed PostgreSQL connection successfully");
		} catch (SQLException exception) {
			LOGGER.warn("Failed to close PostgreSQL resources cleanly", exception);
		}
	}

	private static final class CompletionKey {

		private final int windowSize;
		private final long timestamp;

		private CompletionKey(int windowSize, long timestamp) {
			this.windowSize = windowSize;
			this.timestamp = timestamp;
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (!(other instanceof CompletionKey)) {
				return false;
			}
			CompletionKey key = (CompletionKey) other;
			return windowSize == key.windowSize && timestamp == key.timestamp;
		}

		@Override
		public int hashCode() {
			return 31 * windowSize + Long.hashCode(timestamp);
		}
	}

	private static final class CompletionState {

		private boolean persistenceCompleted;
		private boolean forecastCompleted;
	}

}