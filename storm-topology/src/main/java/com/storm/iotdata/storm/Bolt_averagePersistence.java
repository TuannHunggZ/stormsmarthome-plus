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
import java.util.Map;
import java.util.Objects;

/**
 * Sink bolt that batches plug and house averages into PostgreSQL.
 */
public class Bolt_averagePersistence extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_averagePersistence.class);
	private static final int BATCH_SIZE = 1000;

	private final String inputPlugStreamId;
	private final String inputHouseStreamId;
	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private final int batchSize;
	private final String plugTableName;
	private final String houseTableName;
	private final String plugInsertSql;
	private final String houseInsertSql;

	private transient OutputCollector collector;
	private transient Connection connection;
	private transient PreparedStatement plugStatement;
	private transient PreparedStatement houseStatement;
	private int plugBatchCount;
	private int houseBatchCount;

	public Bolt_averagePersistence(StormConfig.BoltAveragePersistenceConfig config) {
		Objects.requireNonNull(config, "config");
		this.inputPlugStreamId = config.getInputPlugStreamId();
		this.inputHouseStreamId = config.getInputHouseStreamId();
		this.jdbcUrl = config.getJdbcUrl();
		this.jdbcUser = config.getJdbcUser();
		this.jdbcPassword = config.getJdbcPassword();
		this.batchSize = config.getBatchSize() > 0 ? config.getBatchSize() : BATCH_SIZE;
		this.plugTableName = config.getPlugTableName();
		this.houseTableName = config.getHouseTableName();
		this.plugInsertSql = String.format(config.getPlugInsertSql(), plugTableName);
		this.houseInsertSql = String.format(config.getHouseInsertSql(), houseTableName);
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

			if (inputPlugStreamId.equals(sourceStreamId)) {
				processPlugAverage(input);
			} else if (inputHouseStreamId.equals(sourceStreamId)) {
				processHouseAverage(input);
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
		// Sink bolt, no output streams.
	}

	@Override
	public void cleanup() {
		try {
			flushPlugBatch();
			flushHouseBatch();
			closeResources();
		} catch (SQLException exception) {
			LOGGER.error("Failed to cleanly flush persistence batches during cleanup", exception);
			closeResources();
		}
	}

	private void initializeDatabase() {
		try {
			connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
			connection.setAutoCommit(false);
			plugStatement = connection.prepareStatement(plugInsertSql);
			houseStatement = connection.prepareStatement(houseInsertSql);
			LOGGER.info("Connected to PostgreSQL at {}", jdbcUrl);
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

	private void processPlugAverage(Tuple input) throws SQLException {
		plugStatement.setInt(1, input.getIntegerByField("windowSize"));
		plugStatement.setLong(2, input.getLongByField("sliceIndex"));
		plugStatement.setInt(3, input.getIntegerByField("houseId"));
		plugStatement.setInt(4, input.getIntegerByField("householdId"));
		plugStatement.setInt(5, input.getIntegerByField("plugId"));
		plugStatement.setDouble(6, input.getDoubleByField("currentAverage"));
		plugStatement.addBatch();
		plugBatchCount += 1;

		if (plugBatchCount >= batchSize) {
			flushPlugBatch();
		}
	}

	private void processHouseAverage(Tuple input) throws SQLException {
		houseStatement.setInt(1, input.getIntegerByField("windowSize"));
		houseStatement.setLong(2, input.getLongByField("sliceIndex"));
		houseStatement.setInt(3, input.getIntegerByField("houseId"));
		houseStatement.setDouble(4, input.getDoubleByField("currentAverage"));
		houseStatement.addBatch();
		houseBatchCount += 1;

		// House aggregates are infrequent, so flush immediately.
        flushHouseBatch();
	}

	private void flushPlugBatch() throws SQLException {
		if (plugBatchCount == 0) {
			return;
		}

		try {
			int[] results = plugStatement.executeBatch();
			connection.commit();
			plugStatement.clearBatch();
			LOGGER.info("Flushed plug batch with {} records", results.length);
		} catch (SQLException exception) {
			throw exception;
		} finally {
			plugBatchCount = 0;
		}
	}

	private void flushHouseBatch() throws SQLException {
		if (houseBatchCount == 0) {
			return;
		}

		try {
			int[] results = houseStatement.executeBatch();
			connection.commit();
			houseStatement.clearBatch();
			LOGGER.info("Flushed house batch with {} records", results.length);
		} catch (SQLException exception) {
			throw exception;
		} finally {
			houseBatchCount = 0;
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
			if (plugStatement != null) {
				plugStatement.close();
			}
			if (houseStatement != null) {
				houseStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
			LOGGER.info("Closed PostgreSQL resources successfully");
		} catch (SQLException exception) {
			LOGGER.warn("Failed to close PostgreSQL resources cleanly", exception);
		}
	}
}