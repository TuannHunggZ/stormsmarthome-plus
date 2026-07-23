package com.storm.iotdata.storm;

import com.storm.iotdata.models.StormConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that joins house average and archive median to compute house forecast values.
 */
public class Bolt_houseForecast extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_houseForecast.class);
	private static final long SECONDS_PER_DAY = 86_400L;

	private final String inputAverageStreamId;
	private final String inputMedianStreamId;
	private final String inputFieldWindowSize;
	private final String inputFieldSliceIndex;
	private final String inputFieldHouseId;
	private final String inputFieldCurrentAverage;
	private final String inputFieldArchiveMedian;
	private final long minimumDatasetTimestampSeconds;

	private transient OutputCollector collector;
	private final Map<ForecastKey, ForecastState> forecastStates;

	/**
	 * Creates the bolt with configured stream ids and field mappings.
	 *
	 * @param config Shared bolt configuration.
	 */
	public Bolt_houseForecast(StormConfig.BoltHouseForecastConfig config) {
		Objects.requireNonNull(config, "config");
		this.inputAverageStreamId = config.getInputAverageStreamId();
		this.inputMedianStreamId = config.getInputMedianStreamId();
		this.inputFieldWindowSize = config.getInputFieldWindowSize();
		this.inputFieldSliceIndex = config.getInputFieldSliceIndex();
		this.inputFieldHouseId = config.getInputFieldHouseId();
		this.inputFieldCurrentAverage = config.getInputFieldCurrentAverage();
		this.inputFieldArchiveMedian = config.getInputFieldArchiveMedian();
		this.minimumDatasetTimestampSeconds = config.getMinimumDatasetTimestampSeconds();
		this.forecastStates = new HashMap<>();
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String sourceStreamId = input.getSourceStreamId();

		try {
			if (inputAverageStreamId.equals(sourceStreamId)) {
				processAverageTuple(input);
			} else if (inputMedianStreamId.equals(sourceStreamId)) {
				processMedianTuple(input);
			}
			collector.ack(input);
		} catch (RuntimeException exception) {
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
	}

	private void processAverageTuple(Tuple input) {
		ForecastKey forecastKey = readForecastKey(input);
		ForecastState forecastState = forecastStates.computeIfAbsent(forecastKey, ignored -> new ForecastState());
		forecastState.average = input.getDoubleByField(inputFieldCurrentAverage);
		forecastState.hasAverage = true;

		long firstDayLastSlice =
			(minimumDatasetTimestampSeconds + SECONDS_PER_DAY)
				/ (forecastKey.windowSize * 60L);
		if (forecastKey.sliceIndex < firstDayLastSlice) {
			logForecast(forecastKey, forecastState.average, forecastState.median, forecastState.average);
			forecastStates.remove(forecastKey);
			return;
		}

		if (forecastState.hasMedian) {
			double forecast = (forecastState.average + forecastState.median) / 2.0d;
			logForecast(forecastKey, forecastState.average, forecastState.median, forecast);
			forecastStates.remove(forecastKey);
		}
	}

	private void processMedianTuple(Tuple input) {
		ForecastKey forecastKey = readForecastKey(input);
		ForecastState forecastState = forecastStates.computeIfAbsent(forecastKey, ignored -> new ForecastState());
		forecastState.median = input.getDoubleByField(inputFieldArchiveMedian);
		forecastState.hasMedian = true;

		if (forecastState.hasAverage) {
			double forecast = (forecastState.average + forecastState.median) / 2.0d;
			logForecast(forecastKey, forecastState.average, forecastState.median, forecast);
			forecastStates.remove(forecastKey);
		}
	}

	private ForecastKey readForecastKey(Tuple input) {
		return new ForecastKey(
			input.getIntegerByField(inputFieldWindowSize),
			input.getLongByField(inputFieldSliceIndex),
			input.getIntegerByField(inputFieldHouseId)
		);
	}

	private void logForecast(ForecastKey forecastKey, Double average, Double median, double forecast) {
		LOGGER.info(
			"Window={}m Slice={} House={} Average={} Median={} Forecast={}",
			forecastKey.windowSize,
			forecastKey.sliceIndex,
			forecastKey.houseId,
			average,
			median,
			forecast
		);
	}

	private static final class ForecastKey {

		private final int windowSize;
		private final long sliceIndex;
		private final int houseId;

		private ForecastKey(int windowSize, long sliceIndex, int houseId) {
			this.windowSize = windowSize;
			this.sliceIndex = sliceIndex;
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
				&& sliceIndex == forecastKey.sliceIndex
				&& houseId == forecastKey.houseId;
		}

		@Override
		public int hashCode() {
			return Objects.hash(windowSize, sliceIndex, houseId);
		}
	}

	private static final class ForecastState {

		private Double average;
		private Double median;
		private boolean hasAverage;
		private boolean hasMedian;
	}
}