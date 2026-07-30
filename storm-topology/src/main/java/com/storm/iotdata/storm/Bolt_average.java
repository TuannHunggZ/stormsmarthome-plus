package com.storm.iotdata.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that computes the current average load per plug and per house for one configured window size.
 *
 * The bolt keeps only the active slice state in memory and emits results when punctuation closes the slice.
 */
public class Bolt_average extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_average.class);

	private final String inputStreamData;
	private final String inputFieldValue;
	private final String inputFieldPlugId;
	private final String inputFieldHouseholdId;
	private final String inputFieldHouseId;
	private final String inputFieldWindowSize;
	private final String inputFieldTimestamp;

	private final String outputPlugStreamId;
	private final String outputHouseStreamId;
	private final String outputFieldWindowSize;
	private final String outputFieldTimestamp;
	private final String outputFieldHouseId;
	private final String outputFieldHouseholdId;
	private final String outputFieldPlugId;
	private final String outputFieldCurrentAverage;

	private final int windowSizeMinutes;
	private final Map<Integer, Map<PlugKey, AverageAccumulator>> accumulators;
	private transient OutputCollector collector;

	/**
	 * Creates a bolt for a single punctuation window.
	 *
	 * @param windowSizeMinutes Window size handled by this bolt, in minutes.
	 */
	public Bolt_average(int windowSizeMinutes) {

		this.inputStreamData = "data";
		this.inputFieldValue = "value";
		this.inputFieldPlugId = "plugId";
		this.inputFieldHouseholdId = "householdId";
		this.inputFieldHouseId = "houseId";
		this.inputFieldWindowSize = "windowSize";
		this.inputFieldTimestamp = "timestamp";
		this.outputPlugStreamId = "current-plug-average";
		this.outputHouseStreamId = "current-house-average";
		this.outputFieldWindowSize = "windowSize";
		this.outputFieldTimestamp = "timestamp";
		this.outputFieldHouseId = "houseId";
		this.outputFieldHouseholdId = "householdId";
		this.outputFieldPlugId = "plugId";
		this.outputFieldCurrentAverage = "currentAverage";

		this.windowSizeMinutes = windowSizeMinutes;
		this.accumulators = new HashMap<>();
	}

	/**
	 * Prepares the bolt and logs that it is ready to process live data.
	 */
	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		LOGGER.info("Bolt_Average initialized for window {}m", windowSizeMinutes);
	}

	/**
	 * Routes input tuples to the corresponding processing path.
	 */
	@Override
	public void execute(Tuple input) {
		String sourceStreamId = input.getSourceStreamId();

		try {
			if (inputStreamData.equals(sourceStreamId)) {
				processLiveEvent(input);
			} else if (getPunctuationStreamId().equals(sourceStreamId)) {
				processPunctuation(input);
			} else {
				LOGGER.debug("Ignoring tuple from unexpected stream {}", sourceStreamId);
			}
		} finally {
			collector.ack(input);
		}
	}

	/**
	 * Declares the output fields for the plug-average and house-average streams.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(
			outputPlugStreamId,
			new Fields(
				outputFieldWindowSize,
				outputFieldTimestamp,
				outputFieldHouseId,
				outputFieldHouseholdId,
				outputFieldPlugId,
				outputFieldCurrentAverage
			)
		);

		declarer.declareStream(
			outputHouseStreamId,
			new Fields(
				outputFieldWindowSize,
				outputFieldTimestamp,
				outputFieldHouseId,
				outputFieldCurrentAverage
			)
		);
	}

	/**
	 * Clears the in-memory state before the bolt is shut down.
	 */
	@Override
	public void cleanup() {
		accumulators.clear();
	}

	private void processLiveEvent(Tuple input) {
		int houseId = input.getIntegerByField(inputFieldHouseId);
		Map<PlugKey, AverageAccumulator> houseAccumulators = accumulators.computeIfAbsent(
			houseId,
			ignored -> new HashMap<PlugKey, AverageAccumulator>()
		);

		PlugKey key = new PlugKey(
			input.getIntegerByField(inputFieldHouseholdId),
			input.getIntegerByField(inputFieldPlugId)
		);

		AverageAccumulator accumulator = houseAccumulators.computeIfAbsent(
			key,
			ignored -> new AverageAccumulator()
		);
		accumulator.add(input.getDoubleByField(inputFieldValue));
	}

	private void processPunctuation(Tuple input) {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long timestamp = input.getLongByField(inputFieldTimestamp);

		LOGGER.info("Received punctuation for window {}m timestamp {}", windowSize, timestamp);
		LOGGER.info("Processing {} houses for window {}m timestamp {}", accumulators.size(), windowSize, timestamp);

		int emittedPlugCount = 0;
		int emittedHouseCount = 0;

		for (Map.Entry<Integer, Map<PlugKey, AverageAccumulator>> houseEntry : accumulators.entrySet()) {
			emittedPlugCount += emitPlugAverages(windowSize, timestamp, houseEntry.getKey(), houseEntry.getValue());
			emitHouseAverage(windowSize, timestamp, houseEntry.getKey(), houseEntry.getValue());
			emittedHouseCount += 1;
		}

		LOGGER.info("Emitted {} plug averages and {} house averages for window {}m timestamp {}", emittedPlugCount, emittedHouseCount, windowSize, timestamp);
		cleanupProcessedEvents();
	}

	private int emitPlugAverages(int windowSize, long timestamp, int houseId, Map<PlugKey, AverageAccumulator> houseAccumulators) {
		int emittedCount = 0;

		for (Map.Entry<PlugKey, AverageAccumulator> entry : houseAccumulators.entrySet()) {
			PlugKey plugKey = entry.getKey();
			double currentAverage = entry.getValue().average();

			collector.emit(
				outputPlugStreamId,
				new Values(
					windowSize,
					timestamp,
					houseId,
					plugKey.householdId,
					plugKey.plugId,
					currentAverage
				)
			);
			emittedCount += 1;
		}

		return emittedCount;
	}

	private void emitHouseAverage(int windowSize, long timestamp, int houseId, Map<PlugKey, AverageAccumulator> houseAccumulators) {
		double houseAverage = 0.0d;

		for (AverageAccumulator accumulator : houseAccumulators.values()) {
			houseAverage += accumulator.average();
		}

		collector.emit(
			outputHouseStreamId,
			new Values(
				windowSize,
				timestamp,
				houseId,
				houseAverage
			)
		);
	}

	private void cleanupProcessedEvents() {
		int removedCount = accumulators.size();
		accumulators.clear();
		LOGGER.info("Cleared {} house accumulators after processing window {}m", removedCount, windowSizeMinutes);
	}

	private String getPunctuationStreamId() {
		return "punctuation-" + windowSizeMinutes + "m";
	}

	/**
	 * Identifies a plug within a household.
	 */
	private static final class PlugKey {

		private final int householdId;
		private final int plugId;

		private PlugKey(int householdId, int plugId) {
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
			return householdId == plugKey.householdId && plugId == plugKey.plugId;
		}

		@Override
		public int hashCode() {
			return Objects.hash(householdId, plugId);
		}
	}

	/**
	 * Small accumulator used to compute a running average.
	 */
	private static final class AverageAccumulator {

		private double sum;
		private long count;

		private void add(double value) {
			sum += value;
			count += 1;
		}

		private double average() {
			if (count == 0L) {
				return 0.0d;
			}

			return sum / count;
		}
	}
}