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
import java.util.List;
import java.util.Map;

/**
 * Bolt that expands punctuation tuples into per-house median trigger tuples.
 */
public class Bolt_medianTrigger extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_medianTrigger.class);

	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private final String selectHouseIdsSql;
	private final String inputStreamIdPrefix;
	private final String inputFieldWindowSize;
	private final String inputFieldTimestamp;
	private final String outputStreamId;
	private final String outputFieldWindowSize;
	private final String outputFieldTimestamp;
	private final String outputFieldHouseId;

	private transient OutputCollector collector;
	private List<Integer> houseIds;

	public Bolt_medianTrigger() {
		this.jdbcUrl = StormConfig.getJdbcUrl();
		this.jdbcUser = StormConfig.getJdbcUser();
		this.jdbcPassword = StormConfig.getJdbcPassword();
		this.selectHouseIdsSql = String.format(
			StormConfig.getMedianTriggerHouseIdsSelectSqlTemplate(),
			StormConfig.getHouseAverageExpectedTableName()
		);
		this.inputStreamIdPrefix = "punctuation-";
		this.inputFieldWindowSize = "windowSize";
		this.inputFieldTimestamp = "timestamp";
		this.outputStreamId = "median-trigger";
		this.outputFieldWindowSize = "windowSize";
		this.outputFieldTimestamp = "timestamp";
		this.outputFieldHouseId = "houseId";
		this.houseIds = Collections.emptyList();
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.houseIds = loadHouseIds();
		LOGGER.info("Loaded {} house ids for median trigger", houseIds.size());
	}

	@Override
	public void execute(Tuple input) {
		try {
			String sourceStreamId = input.getSourceStreamId();
			if (sourceStreamId != null && sourceStreamId.startsWith(inputStreamIdPrefix)) {
				emitPerHouseTrigger(input);
			}
			collector.ack(input);
		} catch (RuntimeException exception) {
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(
			outputStreamId,
			new Fields(outputFieldWindowSize, outputFieldTimestamp, outputFieldHouseId)
		);
	}

	private List<Integer> loadHouseIds() {
		List<Integer> loadedHouseIds = new ArrayList<>();

		try (
			Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
			PreparedStatement statement = connection.prepareStatement(selectHouseIdsSql);
			ResultSet resultSet = statement.executeQuery()
		) {
			while (resultSet.next()) {
				loadedHouseIds.add(resultSet.getInt(1));
			}
			return Collections.unmodifiableList(loadedHouseIds);
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to load house ids for median trigger", exception);
		}
	}

	private void emitPerHouseTrigger(Tuple input) {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long timestamp = input.getLongByField(inputFieldTimestamp);

		for (Integer houseId : houseIds) {
			collector.emit(outputStreamId, new Values(windowSize, timestamp, houseId));
		}

		LOGGER.info(
			"Expanded punctuation window {}m at {} into {} house triggers",
			windowSize,
			timestamp,
			houseIds.size()
		);
	}
}