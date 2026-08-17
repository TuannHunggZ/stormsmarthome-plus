package com.storm.iotdata.storm;

import com.storm.iotdata.models.RedisAnomalyPublisher;
import com.storm.iotdata.models.StormConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that detects house anomalies from current house averages while maintaining rolling statistics in memory.
 */
public class Bolt_houseAnomalyDetection extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_houseAnomalyDetection.class);

	private final String inputStreamId;
	private final String inputFieldWindowSize;
	private final String inputFieldTimestamp;
	private final String inputFieldHouseId;
	private final String inputFieldCurrentAverage;
	private final int anomalyThresholdPercent;
	private final boolean houseCheckMax;
	private final boolean houseCheckAvg;
	private final boolean houseCheckMin;
	private final Map<HouseKey, RollingStatistic> rollingStatistics;
	private transient RedisAnomalyPublisher redisPublisher;
	private transient OutputCollector collector;

	/**
	 * Creates the bolt for house anomaly detection.
	 */
	public Bolt_houseAnomalyDetection() {
		this.inputStreamId = "current-house-average";
		this.inputFieldWindowSize = "windowSize";
		this.inputFieldTimestamp = "timestamp";
		this.inputFieldHouseId = "houseId";
		this.inputFieldCurrentAverage = "currentAverage";
		this.anomalyThresholdPercent = StormConfig.getAnomalyThresholdPercent();
		this.houseCheckMax = StormConfig.isHouseCheckMax();
		this.houseCheckAvg = StormConfig.isHouseCheckAvg();
		this.houseCheckMin = StormConfig.isHouseCheckMin();
		this.rollingStatistics = new HashMap<>();
	}

    @Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.redisPublisher = new RedisAnomalyPublisher();
		redisPublisher.initialize();
		LOGGER.info("Bolt_HouseAnomalyDetection initialized with threshold {}%", anomalyThresholdPercent);
	}

    @Override
	public void execute(Tuple input) {
		try {
			if (inputStreamId.equals(input.getSourceStreamId())) {
				processAverage(input);
			} else {
				LOGGER.debug("Ignoring tuple from unexpected stream {}", input.getSourceStreamId());
			}
		} finally {
			collector.ack(input);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Sink bolt, no output streams.
	}

	@Override
	public void cleanup() {
		rollingStatistics.clear();
		redisPublisher.close();
	}

    private void processAverage(Tuple input) {
		int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long timestamp = input.getLongByField(inputFieldTimestamp);
		int houseId = input.getIntegerByField(inputFieldHouseId);
		double currentAverage = input.getDoubleByField(inputFieldCurrentAverage);

		if (currentAverage == 0.0d) {
			LOGGER.debug(
				"Skipping house anomaly statistics update for zero value windowSize={} timestamp={} houseId={}",
				windowSize,
				timestamp,
				houseId
			);
			return;
		}

		HouseKey houseKey = new HouseKey(windowSize, houseId);
		RollingStatistic statistic = rollingStatistics.computeIfAbsent(houseKey, ignored -> new RollingStatistic());

		if (statistic.count == 0L) {
			statistic.initialize(currentAverage);
			return;
		}

		checkAnomalies(windowSize, timestamp, houseId, currentAverage, statistic);
		statistic.update(currentAverage);
	}

    private void checkAnomalies(int windowSize, long timestamp, int houseId, double currentAverage, RollingStatistic statistic) {
        if (houseCheckMax && statistic.max != 0.0d && (currentAverage - statistic.max) >= statistic.max * anomalyThresholdPercent / 100.0d) {
			logAnomaly("MAX", windowSize, timestamp, houseId, currentAverage, statistic);
		}

		if (houseCheckAvg && statistic.avg != 0.0d && (currentAverage - statistic.avg) >= statistic.avg * anomalyThresholdPercent / 100.0d) {
			logAnomaly("AVG", windowSize, timestamp, houseId, currentAverage, statistic);
		}

		if (houseCheckMin && statistic.min != 0.0d && (statistic.min - currentAverage) >= statistic.min * anomalyThresholdPercent / 100.0d) {
			logAnomaly("MIN", windowSize, timestamp, houseId, currentAverage, statistic);
		}
    }

    private void logAnomaly(String anomalyType, int windowSize, long timestamp, int houseId, double currentAverage, RollingStatistic statistic) {
        publishAnomalyEvent(anomalyType, windowSize, timestamp, houseId, currentAverage, statistic);
		LOGGER.info(
			"TODO anomaly handling: type={} windowSize={} timestamp={} houseId={} value={} avg={} min={} max={} anomalyThresholdPercent={}",
			anomalyType,
			windowSize,
			timestamp,
			houseId,
			currentAverage,
			statistic.avg,
			statistic.min,
			statistic.max,
			anomalyThresholdPercent
		);
    }

	private void publishAnomalyEvent(String anomalyType, int windowSize, long timestamp, int houseId, double currentAverage, RollingStatistic statistic) {
		Map<String, Object> event = new LinkedHashMap<>();
		event.put("type", "HOUSE_ANOMALY");
		event.put("anomalyType", anomalyType);
		event.put("windowSize", windowSize);
		event.put("timestamp", timestamp);
		event.put("houseId", houseId);
		event.put("householdId", null);
		event.put("plugId", null);
		event.put("value", currentAverage);
		event.put("avg", statistic.avg);
		event.put("min", statistic.min);
		event.put("max", statistic.max);
		event.put("anomalyThresholdPercent", anomalyThresholdPercent);

		redisPublisher.publish(StormConfig.getHouseAnomalyChannel(), event);
	}

    private static final class HouseKey {

		private final int windowSize;
		private final int houseId;

		private HouseKey(int windowSize, int houseId) {
			this.windowSize = windowSize;
			this.houseId = houseId;
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (!(other instanceof HouseKey)) {
				return false;
			}
			HouseKey houseKey = (HouseKey) other;
			return windowSize == houseKey.windowSize && houseId == houseKey.houseId;
		}

		@Override
		public int hashCode() {
			return Objects.hash(windowSize, houseId);
		}
	}

	private static final class RollingStatistic {

		private double min;
		private double max;
		private double avg;
		private long count;

		private void initialize(double value) {
			min = value;
			max = value;
			avg = value;
			count = 1L;
		}

		private void update(double value) {
			long updatedCount = count + 1L;
			avg = (avg * count + value) / updatedCount;
			count = updatedCount;
			min = Math.min(min, value);
			max = Math.max(max, value);
		}
	}
}
