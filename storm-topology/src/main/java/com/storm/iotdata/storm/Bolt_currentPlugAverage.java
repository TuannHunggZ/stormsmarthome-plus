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
 * Bolt that computes the current average load per plug for one configured window size.
 *
 * The bolt stores live events in memory and only emits results when a punctuation tuple
 * confirms that a slice has been fully received.
 */
public class Bolt_currentPlugAverage extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_currentPlugAverage.class);

	private final String inputStreamData;
	private final String inputFieldValue;
	private final String inputFieldPlugId;
	private final String inputFieldHouseholdId;
	private final String inputFieldHouseId;
	private final String inputFieldWindowSize;
	private final String inputFieldSliceIndex;

	private final String outputFieldWindowSize;
	private final String outputFieldSliceIndex;
	private final String outputFieldHouseId;
	private final String outputFieldHouseholdId;
	private final String outputFieldPlugId;
	private final String outputFieldCurrentAverage;

	private final int windowSizeMinutes;
	private final Map<PlugKey, AverageAccumulator> accumulators;
	private transient OutputCollector collector;

	/**
	 * Creates a bolt for a single punctuation window.
	 *
	 * @param stormConfig Shared Storm configuration.
	 * @param windowSizeMinutes Window size handled by this bolt, in minutes.
	 */
	public Bolt_currentPlugAverage(StormConfig.BoltCurrentPlugAverageConfig boltCurrentPlugAverageConfig, int windowSizeMinutes) {

		this.inputStreamData = boltCurrentPlugAverageConfig.getInputStreamData();
		this.inputFieldValue = boltCurrentPlugAverageConfig.getInputFieldValue();
		this.inputFieldPlugId = boltCurrentPlugAverageConfig.getInputFieldPlugId();
		this.inputFieldHouseholdId = boltCurrentPlugAverageConfig.getInputFieldHouseholdId();
		this.inputFieldHouseId = boltCurrentPlugAverageConfig.getInputFieldHouseId();
		this.inputFieldWindowSize = boltCurrentPlugAverageConfig.getInputFieldWindowSize();
		this.inputFieldSliceIndex = boltCurrentPlugAverageConfig.getInputFieldSliceIndex();
		this.outputFieldWindowSize = boltCurrentPlugAverageConfig.getOutputFieldWindowSize();
		this.outputFieldSliceIndex = boltCurrentPlugAverageConfig.getOutputFieldSliceIndex();
		this.outputFieldHouseId = boltCurrentPlugAverageConfig.getOutputFieldHouseId();
		this.outputFieldHouseholdId = boltCurrentPlugAverageConfig.getOutputFieldHouseholdId();
		this.outputFieldPlugId = boltCurrentPlugAverageConfig.getOutputFieldPlugId();
		this.outputFieldCurrentAverage = boltCurrentPlugAverageConfig.getOutputFieldCurrentAverage();

		this.windowSizeMinutes = windowSizeMinutes;
		this.accumulators = new HashMap<>();
	}

	/**
	 * Prepares the bolt and logs that it is ready to process live data.
	 */
	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		LOGGER.info("Bolt_currentPlugAverage initialized for window {}m", windowSizeMinutes);
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
	 * Declares the output fields for the current average stream.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
			outputFieldWindowSize,
			outputFieldSliceIndex,
			outputFieldHouseId,
			outputFieldHouseholdId,
			outputFieldPlugId,
			outputFieldCurrentAverage
		));
	}

	/**
	 * Clears the in-memory state before the bolt is shut down.
	 */
	@Override
	public void cleanup() {
		accumulators.clear();
	}

	private void processLiveEvent(Tuple input) {
		PlugKey key = new PlugKey(
			input.getIntegerByField(inputFieldHouseId),
			input.getIntegerByField(inputFieldHouseholdId),
			input.getIntegerByField(inputFieldPlugId)
		);

		AverageAccumulator accumulator = accumulators.computeIfAbsent(key, ignored -> new AverageAccumulator());
		accumulator.add(input.getDoubleByField(inputFieldValue));
	}

	private void processPunctuation(Tuple input) {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long sliceIndex = input.getLongByField(inputFieldSliceIndex);

		LOGGER.info("Received punctuation for window {}m sliceIndex {}", windowSize, sliceIndex);

		LOGGER.info(
			"Processing {} plug accumulators for window {}m sliceIndex {}",
			accumulators.size(),
			windowSize,
			sliceIndex
		);

		Map<PlugKey, Double> currentAverages = calculateCurrentAverage();
		emitCurrentAverage(windowSize, sliceIndex, currentAverages);
		cleanupProcessedEvents();
	}

	private Map<PlugKey, Double> calculateCurrentAverage() {
		Map<PlugKey, Double> averages = new HashMap<>();
		for (Map.Entry<PlugKey, AverageAccumulator> entry : accumulators.entrySet()) {
			averages.put(entry.getKey(), entry.getValue().average());
		}

		return averages;
	}

	private void emitCurrentAverage(int windowSize, long sliceIndex, Map<PlugKey, Double> currentAverages) {
		int emittedCount = 0;

		for (Map.Entry<PlugKey, Double> entry : currentAverages.entrySet()) {
			PlugKey plugKey = entry.getKey();
			double currentAverage = entry.getValue();

			collector.emit(new Values(
				windowSize,
				sliceIndex,
				plugKey.houseId,
				plugKey.householdId,
				plugKey.plugId,
				currentAverage
			));
			emittedCount += 1;
		}

		LOGGER.info(
			"Emitted {} plug averages for window {}m sliceIndex {}",
			emittedCount,
			windowSize,
			sliceIndex
		);
	}

	private void cleanupProcessedEvents() {
		int removedCount = accumulators.size();
		accumulators.clear();

		LOGGER.info("Cleared {} plug accumulators after processing window {}m", removedCount, windowSizeMinutes);
	}

	private String getPunctuationStreamId() {
		return "punctuation-" + windowSizeMinutes + "m";
	}

	/**
	 * Identifies a plug within a house and household.
	 */
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