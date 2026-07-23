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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that computes historical median load per house using house_average.
 */
public class Bolt_houseMedian extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_houseMedian.class);
	private static final int MINUTES_PER_DAY = 1440;

	private final String inputFieldWindowSize;
	private final String inputFieldSliceIndex;
	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private final String selectSqlTemplate;
	private final String inputStreamIdPrefix;
	private final String outputStreamId;
	private final String outputFieldWindowSize;
	private final String outputFieldSliceIndex;
	private final String outputFieldHouseId;
	private final String outputFieldArchiveMedian;
	private final long minimumDatasetTimestampSeconds;

	private transient OutputCollector collector;
	private transient Connection connection;
	private transient PreparedStatement selectStatement;

	/**
	 * Creates the bolt with configured SQL and field mappings.
	 *
	 * @param config Shared bolt configuration.
	 */
	public Bolt_houseMedian(StormConfig.BoltHouseMedianConfig config) {
		Objects.requireNonNull(config, "config");
		this.inputFieldWindowSize = config.getInputFieldWindowSize();
		this.inputFieldSliceIndex = config.getInputFieldSliceIndex();
		this.jdbcUrl = config.getJdbcUrl();
		this.jdbcUser = config.getJdbcUser();
		this.jdbcPassword = config.getJdbcPassword();
		this.selectSqlTemplate = config.getSelectSqlTemplate();
		this.inputStreamIdPrefix = config.getInputStreamIdPrefix();
		this.outputStreamId = config.getOutputStreamId();
		this.outputFieldWindowSize = config.getOutputFieldWindowSize();
		this.outputFieldSliceIndex = config.getOutputFieldSliceIndex();
		this.outputFieldHouseId = config.getOutputFieldHouseId();
		this.outputFieldArchiveMedian = config.getOutputFieldArchiveMedian();
		this.minimumDatasetTimestampSeconds = config.getMinimumDatasetTimestampSeconds();
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		initializeDatabase();
	}

	@Override
	public void execute(Tuple input) {
		try {
			if (input.getSourceStreamId() != null && input.getSourceStreamId().startsWith(inputStreamIdPrefix)) {
				processPunctuation(input);
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
                outputFieldSliceIndex,
                outputFieldHouseId,
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
			selectStatement = connection.prepareStatement(String.format(selectSqlTemplate, "house_average"));
			LOGGER.info("Connected to PostgreSQL successfully");
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

	private void processPunctuation(Tuple input) throws SQLException {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long sliceIndex = input.getLongByField(inputFieldSliceIndex);
		long forecastSlice = sliceIndex + 2L;
		int slicesPerDay = calculateSlicesPerDay(windowSize);
		long windowSizeSeconds = windowSize * 60L;
		long minimumSliceIndex = minimumDatasetTimestampSeconds / windowSizeSeconds;

		LOGGER.info("Forecast slice {} for window {}m", forecastSlice, windowSize);

		Map<Integer, List<Double>> historicalValues = loadHistoricalValues(windowSize, forecastSlice, slicesPerDay, minimumSliceIndex);
		int processedHouseCount = emitMedian(windowSize, sliceIndex, historicalValues);

		LOGGER.info("Number of houses processed: {}", processedHouseCount);
	}

	private int calculateSlicesPerDay(int windowSize) {
		if (windowSize <= 0) {
			throw new IllegalArgumentException("Window size must be positive: " + windowSize);
		}

		return MINUTES_PER_DAY / windowSize;
	}

	private Map<Integer, List<Double>> loadHistoricalValues(int windowSize, long forecastSlice, int slicesPerDay, long minimumSliceIndex) throws SQLException {
		Map<Integer, List<Double>> historicalValues = new HashMap<>();
		int queriedSlices = 0;

		for (long historySlice = forecastSlice - slicesPerDay; historySlice >= minimumSliceIndex; historySlice -= slicesPerDay) {
			loadHistoricalValues(windowSize, historySlice, historicalValues);
			queriedSlices += 1;
		}

		LOGGER.info("History slices queried: {}", queriedSlices);
		return historicalValues;
	}

	private void loadHistoricalValues(int windowSize, long historySlice, Map<Integer, List<Double>> historicalValues) throws SQLException {
		selectStatement.clearParameters();
		selectStatement.setInt(1, windowSize);
		selectStatement.setLong(2, historySlice);

		try (ResultSet resultSet = selectStatement.executeQuery()) {
			while (resultSet.next()) {
				int houseId = resultSet.getInt(1);

				List<Double> values = historicalValues.computeIfAbsent(
					houseId,
					ignored -> new ArrayList<>()
				);

				values.add(resultSet.getDouble(2));
			}
		}
	}

	private int emitMedian(int windowSize, long sliceIndex, Map<Integer, List<Double>> historicalValues) {
		int emittedCount = 0;

		for (Map.Entry<Integer, List<Double>> entry : historicalValues.entrySet()) {
			int houseId = entry.getKey();
			double archiveMedian = calculateMedian(entry.getValue());
			emitMedian(windowSize, sliceIndex, houseId, archiveMedian);
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

	private void emitMedian(int windowSize, long sliceIndex, int houseId, double archiveMedian) {
		collector.emit(
			outputStreamId,
			new Values(
				windowSize,
				sliceIndex,
				houseId,
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
}