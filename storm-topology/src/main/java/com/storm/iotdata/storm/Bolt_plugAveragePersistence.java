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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

/** Persists plug averages and flushes them when a punctuation is received. */
public class Bolt_plugAveragePersistence extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_plugAveragePersistence.class);

	private final String inputAverageStreamId = "current-plug-average";
	private final String inputFieldWindowSize = "windowSize";
	private final String inputFieldTimestamp = "timestamp";
	private final String outputCompletionStreamId = "plug-average-persisted";
	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private final String insertSql;

	private transient OutputCollector collector;
	private transient Connection connection;
	private transient PreparedStatement statement;
	private int batchCount;

	public Bolt_plugAveragePersistence() {
		this.jdbcUrl = StormConfig.getJdbcUrl();
		this.jdbcUser = StormConfig.getJdbcUser();
		this.jdbcPassword = StormConfig.getJdbcPassword();
		this.insertSql = String.format(
			StormConfig.getPlugAverageInsertSql(),
			StormConfig.getPlugAverageTableName()
		);
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		initializeDatabase();
	}

	@Override
	public void execute(Tuple input) {
		try {
			if (inputAverageStreamId.equals(input.getSourceStreamId())) {
				addAverage(input);
			} else if (isPunctuation(input)) {
				flushBatch(input);
			}
			collector.ack(input);
		} catch (SQLException | RuntimeException exception) {
			rollbackTransaction(exception);
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(
			outputCompletionStreamId,
			new Fields(inputFieldWindowSize, inputFieldTimestamp)
		);
	}

	@Override
	public void cleanup() {
		try {
			flushBatch();
		} catch (SQLException exception) {
			LOGGER.error("Failed to flush plug averages during cleanup", exception);
		} finally {
			closeResources();
		}
	}

	private void initializeDatabase() {
		try {
			connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
			connection.setAutoCommit(false);
			statement = connection.prepareStatement(insertSql);
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

	private void addAverage(Tuple input) throws SQLException {
		statement.setInt(1, input.getIntegerByField("windowSize"));
		statement.setInt(2, input.getIntegerByField("sliceIndexInDay"));
		statement.setTimestamp(3, toSqlTimestamp(input.getLongByField("timestamp")));
		statement.setInt(4, input.getIntegerByField("houseId"));
		statement.setInt(5, input.getIntegerByField("householdId"));
		statement.setInt(6, input.getIntegerByField("plugId"));
		statement.setDouble(7, input.getDoubleByField("currentAverage"));
		statement.addBatch();
		batchCount += 1;
	}

	private boolean isPunctuation(Tuple input) {
		return input.getSourceStreamId() != null
			&& input.getSourceStreamId().startsWith("punctuation-")
			&& input.contains(inputFieldWindowSize)
			&& input.contains(inputFieldTimestamp);
	}

	private void flushBatch() throws SQLException {
		flushBatch(null);
	}

	private void flushBatch(Tuple punctuation) throws SQLException {
		if (batchCount == 0) {
			emitCompletion(punctuation);
			return;
		}

		statement.executeBatch();
		connection.commit();
		statement.clearBatch();
		LOGGER.info("Flushed {} plug average records", batchCount);
		batchCount = 0;

		emitCompletion(punctuation);
	}

	private void emitCompletion(Tuple punctuation) {
		if (punctuation != null) {
			collector.emit(
				outputCompletionStreamId,
				new Values(
					punctuation.getIntegerByField(inputFieldWindowSize),
					punctuation.getLongByField(inputFieldTimestamp)
				)
			);
		}
	}

	private void rollbackTransaction(Exception exception) {
		try {
			if (connection != null) {
				connection.rollback();
			}
		} catch (SQLException rollbackException) {
			LOGGER.error("Failed to rollback plug average transaction", rollbackException);
		}
		LOGGER.error("Failed to persist plug average", exception);
	}

	private void closeResources() {
		try {
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException exception) {
			LOGGER.warn("Failed to close plug persistence resources", exception);
		}
	}

	private Timestamp toSqlTimestamp(long epochSeconds) {
		return Timestamp.from(Instant.ofEpochSecond(epochSeconds));
	}
}