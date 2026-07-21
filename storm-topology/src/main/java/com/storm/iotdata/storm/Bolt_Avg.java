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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that computes the current average load per plug and per house for one configured window size.
 *
 * The bolt keeps only the active slice state in memory and emits results when punctuation closes the slice.
 */
public class Bolt_Avg extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_Avg.class);

	private final String inputStreamData;
	private final String inputFieldValue;
	private final String inputFieldPlugId;
	private final String inputFieldHouseholdId;
	private final String inputFieldHouseId;
	private final String inputFieldWindowSize;
	private final String inputFieldSliceIndex;

	private final String outputPlugStreamId;
	private final String outputHouseStreamId;
	private final String outputFieldWindowSize;
	private final String outputFieldSliceIndex;
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
	 * @param boltAvgConfig Shared bolt configuration.
	 * @param windowSizeMinutes Window size handled by this bolt, in minutes.
	 */
	public Bolt_Avg(StormConfig.BoltAvgConfig boltAvgConfig, int windowSizeMinutes) {
		Objects.requireNonNull(boltAvgConfig, "boltAvgConfig");

		this.inputStreamData = boltAvgConfig.getInputStreamData();
		this.inputFieldValue = boltAvgConfig.getInputFieldValue();
		this.inputFieldPlugId = boltAvgConfig.getInputFieldPlugId();
		this.inputFieldHouseholdId = boltAvgConfig.getInputFieldHouseholdId();
		this.inputFieldHouseId = boltAvgConfig.getInputFieldHouseId();
		this.inputFieldWindowSize = boltAvgConfig.getInputFieldWindowSize();
		this.inputFieldSliceIndex = boltAvgConfig.getInputFieldSliceIndex();
		this.outputPlugStreamId = boltAvgConfig.getOutputPlugStreamId();
		this.outputHouseStreamId = boltAvgConfig.getOutputHouseStreamId();
		this.outputFieldWindowSize = boltAvgConfig.getOutputFieldWindowSize();
		this.outputFieldSliceIndex = boltAvgConfig.getOutputFieldSliceIndex();
		this.outputFieldHouseId = boltAvgConfig.getOutputFieldHouseId();
		this.outputFieldHouseholdId = boltAvgConfig.getOutputFieldHouseholdId();
		this.outputFieldPlugId = boltAvgConfig.getOutputFieldPlugId();
		this.outputFieldCurrentAverage = boltAvgConfig.getOutputFieldCurrentAverage();

		this.windowSizeMinutes = windowSizeMinutes;
		this.accumulators = new HashMap<>();
	}

	/**
	 * Prepares the bolt and logs that it is ready to process live data.
	 */
	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		LOGGER.info("Bolt_Avg initialized for window {}m", windowSizeMinutes);
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
				outputFieldSliceIndex,
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
				outputFieldSliceIndex,
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
		long sliceIndex = input.getLongByField(inputFieldSliceIndex);

		LOGGER.info("Received punctuation for window {}m sliceIndex {}", windowSize, sliceIndex);
		LOGGER.info("Processing {} houses for window {}m sliceIndex {}", accumulators.size(), windowSize, sliceIndex);

		int emittedPlugCount = 0;
		int emittedHouseCount = 0;

		for (Map.Entry<Integer, Map<PlugKey, AverageAccumulator>> houseEntry : accumulators.entrySet()) {
			emittedPlugCount += emitPlugAverages(windowSize, sliceIndex, houseEntry.getKey(), houseEntry.getValue());
			emitHouseAverage(windowSize, sliceIndex, houseEntry.getKey(), houseEntry.getValue());
			emittedHouseCount += 1;
		}

		LOGGER.info("Emitted {} plug averages and {} house averages for window {}m sliceIndex {}", emittedPlugCount, emittedHouseCount, windowSize, sliceIndex);
		cleanupProcessedEvents();
	}

	private int emitPlugAverages(int windowSize, long sliceIndex, int houseId, Map<PlugKey, AverageAccumulator> houseAccumulators) {
		int emittedCount = 0;

		for (Map.Entry<PlugKey, AverageAccumulator> entry : houseAccumulators.entrySet()) {
			PlugKey plugKey = entry.getKey();
			double currentAverage = entry.getValue().average();

			collector.emit(
				outputPlugStreamId,
				new Values(
					windowSize,
					sliceIndex,
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

	private void emitHouseAverage(int windowSize, long sliceIndex, int houseId, Map<PlugKey, AverageAccumulator> houseAccumulators) {
		double houseAverage = 0.0d;

		for (AverageAccumulator accumulator : houseAccumulators.values()) {
			houseAverage += accumulator.average();
		}

		collector.emit(
			outputHouseStreamId,
			new Values(
				windowSize,
				sliceIndex,
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