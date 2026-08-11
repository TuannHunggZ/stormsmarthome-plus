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
 * Bolt that detects plug anomalies from current plug averages while maintaining rolling statistics in memory.
 */
public class Bolt_plugAnomalyDetection extends BaseRichBolt {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_plugAnomalyDetection.class);

	private final String inputStreamId;
	private final String inputFieldWindowSize;
	private final String inputFieldTimestamp;
	private final String inputFieldHouseId;
	private final String inputFieldHouseholdId;
	private final String inputFieldPlugId;
	private final String inputFieldCurrentAverage;
	private final int anomalyThresholdPercent;
	private final boolean plugCheckMax;
	private final boolean plugCheckAvg;
	private final boolean plugCheckMin;
	private final Map<PlugKey, RollingStatistic> rollingStatistics;
	private transient OutputCollector collector;

    /**
	 * Creates the bolt for plug anomaly detection.
	 */
	public Bolt_plugAnomalyDetection() {
		this.inputStreamId = "current-plug-average";
		this.inputFieldWindowSize = "windowSize";
		this.inputFieldTimestamp = "timestamp";
		this.inputFieldHouseId = "houseId";
		this.inputFieldHouseholdId = "householdId";
		this.inputFieldPlugId = "plugId";
		this.inputFieldCurrentAverage = "currentAverage";
		this.anomalyThresholdPercent = StormConfig.getAnomalyThresholdPercent();
		this.plugCheckMax = StormConfig.isPlugCheckMax();
		this.plugCheckAvg = StormConfig.isPlugCheckAvg();
		this.plugCheckMin = StormConfig.isPlugCheckMin();
		this.rollingStatistics = new HashMap<>();
	}

    @Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		LOGGER.info("Bolt_PlugAnomalyDetection initialized with threshold {}%", anomalyThresholdPercent);
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
	}

    private void processAverage(Tuple input) {
        int windowSize = input.getIntegerByField(inputFieldWindowSize);
		long timestamp = input.getLongByField(inputFieldTimestamp);
		int houseId = input.getIntegerByField(inputFieldHouseId);
		int householdId = input.getIntegerByField(inputFieldHouseholdId);
		int plugId = input.getIntegerByField(inputFieldPlugId);
		double currentAverage = input.getDoubleByField(inputFieldCurrentAverage);

        if (currentAverage == 0.0d) {
			LOGGER.debug(
				"Skipping plug anomaly statistics update for zero value windowSize={} timestamp={} houseId={} householdId={} plugId={}",
				windowSize,
				timestamp,
				houseId,
				householdId,
				plugId
			);
			return;
		}

        PlugKey plugKey = new PlugKey(windowSize, houseId, householdId, plugId);
        RollingStatistic statistic = rollingStatistics.computeIfAbsent(plugKey, ignored -> new RollingStatistic());

        if (statistic.count == 0L) {
			statistic.initialize(currentAverage);
			return;
		}

        checkAnomalies(windowSize, timestamp, houseId, householdId, plugId, currentAverage, statistic);
		statistic.update(currentAverage);
    }

    private void checkAnomalies(int windowSize, long timestamp, int houseId, int householdId, int plugId, double currentAverage, RollingStatistic statistic) {
        if (plugCheckMax && statistic.max != 0.0d && (currentAverage - statistic.max) >= statistic.max * anomalyThresholdPercent / 100.0d) {
			logAnomaly("MAX", windowSize, timestamp, houseId, householdId, plugId, currentAverage, statistic);
		}

		if (plugCheckAvg && statistic.avg != 0.0d && (currentAverage - statistic.avg) >= statistic.avg * anomalyThresholdPercent / 100.0d) {
			logAnomaly("AVG", windowSize, timestamp, houseId, householdId, plugId, currentAverage, statistic);
		}

		if (plugCheckMin && statistic.min != 0.0d && (statistic.min - currentAverage) >= statistic.min * anomalyThresholdPercent / 100.0d) {
			logAnomaly("MIN", windowSize, timestamp, houseId, householdId, plugId, currentAverage, statistic);
		}
    }

    private void logAnomaly(String anomalyType, int windowSize, long timestamp, int houseId, int householdId, int plugId, double currentAverage, RollingStatistic statistic) {
        LOGGER.info(
			"TODO anomaly handling: type={} windowSize={} timestamp={} houseId={} householdId={} plugId={} value={} avg={} min={} max={} anomalyThresholdPercent={}",
			anomalyType,
			windowSize,
			timestamp,
			houseId,
			householdId,
			plugId,
			currentAverage,
			statistic.avg,
			statistic.min,
			statistic.max,
			anomalyThresholdPercent
		);
    }

    private static final class PlugKey {

		private final int windowSize;
		private final int houseId;
		private final int householdId;
		private final int plugId;

		private PlugKey(int windowSize, int houseId, int householdId, int plugId) {
			this.windowSize = windowSize;
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
			return windowSize == plugKey.windowSize
				&& houseId == plugKey.houseId
				&& householdId == plugKey.householdId
				&& plugId == plugKey.plugId;
		}

		@Override
		public int hashCode() {
			return Objects.hash(windowSize, houseId, householdId, plugId);
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
