package com.storm.iotdata.storm;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
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

import java.util.Iterator;
import java.util.Map;

/**
 * Bolt that keeps the live load events in Esper and emits the current average
 * load per plug only when a punctuation marks the end of a time slice.
 *
 * <p>This bolt implements Bolt-4 from the DEBS Grand Challenge 2014 flow.
 * It does not compute medians and it does not use PostgreSQL or Redis.</p>
 */
public class Bolt_currentPlugAverage extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_currentPlugAverage.class);

    private static final String LIVE_STREAM_ID = "data";
    private static final String FIELD_ID = "id";
    private static final String FIELD_TIMESTAMP = "timestamp";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_PLUG_ID = "plugId";
    private static final String FIELD_HOUSEHOLD_ID = "householdId";
    private static final String FIELD_HOUSE_ID = "houseId";
    private static final String FIELD_WINDOW_SIZE = "windowSize";
    private static final String FIELD_SLICE_INDEX = "sliceIndex";
    private static final String FIELD_CURRENT_AVERAGE = "currentAverage";

    private final int windowSizeMinutes;
    private final String punctuationStreamId;

    private transient OutputCollector collector;
    private transient EPServiceProvider esperServiceProvider;
    private transient EPAdministrator esperAdministrator;
    private transient EPStatement liveEventIngestStatement;
    private transient EPStatement punctuationCleanupStatement;

    /**
     * Creates the bolt using the default Storm configuration file.
     */
    public Bolt_currentPlugAverage() {
        this(new StormConfig(), new StormConfig().getTimeSlicesMinutes().get(0));
    }

    /**
     * Creates the bolt using the provided Storm configuration.
     *
     * @param stormConfig Loaded Storm configuration.
     */
    public Bolt_currentPlugAverage(StormConfig stormConfig) {
        this(stormConfig, stormConfig.getTimeSlicesMinutes().get(0));
    }

    /**
     * Creates the bolt for a single time slice.
     *
     * @param stormConfig Loaded Storm configuration.
     * @param windowSizeMinutes Window size in minutes handled by this bolt instance.
     */
    public Bolt_currentPlugAverage(StormConfig stormConfig, int windowSizeMinutes) {
        this.windowSizeMinutes = windowSizeMinutes;
        this.punctuationStreamId = "punctuation-" + windowSizeMinutes + "m";
    }

    /**
     * Initializes Esper, declares the named window, and registers the EPL statements.
     *
     * @param stormConf Storm configuration map.
     * @param context Topology context.
     * @param collector Storm output collector.
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        initializeEsper();
        registerStatements();
        LOGGER.info("Esper initialized successfully for Bolt_currentPlugAverage window={}m", windowSizeMinutes);
    }

    /**
     * Processes either live data tuples or punctuation tuples.
     *
     * @param input Incoming Storm tuple.
     */
    @Override
    public void execute(Tuple input) {
        String sourceStreamId = input.getSourceStreamId();

        if (LIVE_STREAM_ID.equals(sourceStreamId)) {
            processLiveEvent(input);
            return;
        }

        if (punctuationStreamId.equals(sourceStreamId)) {
            processPunctuation(input);
            return;
        }

        collector.ack(input);
    }

    /**
     * Releases Esper resources and destroys registered statements.
     */
    @Override
    public void cleanup() {
        cleanupStatements();
    }

    /**
     * Declares the output fields for the current average stream.
     *
     * @param declarer Storm declarer.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
            "current-plug-average",
            new Fields(
                FIELD_WINDOW_SIZE,
                FIELD_SLICE_INDEX,
                FIELD_HOUSE_ID,
                FIELD_HOUSEHOLD_ID,
                FIELD_PLUG_ID,
                FIELD_CURRENT_AVERAGE
            )
        );
    }

    /**
     * Initializes the Esper runtime and registers the LiveEvent and punctuation event types.
     */
    private void initializeEsper() {
        Configuration configuration = new Configuration();
        configuration.addEventType(LiveEvent.class.getSimpleName(), LiveEvent.class);
        configuration.addEventType(PunctuationCleanupEvent.class.getSimpleName(), PunctuationCleanupEvent.class);

        esperServiceProvider = EPServiceProviderManager.getProvider("current-plug-average-" + windowSizeMinutes + "m", configuration);
        esperAdministrator = esperServiceProvider.getEPAdministrator();
    }

    /**
     * Registers the named window, ingest statement, and punctuation cleanup statement.
     */
    private void registerStatements() {
        esperAdministrator.createEPL("create window LiveEventWindow.win:keepall() as " + LiveEvent.class.getSimpleName());
        liveEventIngestStatement = esperAdministrator.createEPL("insert into LiveEventWindow select * from " + LiveEvent.class.getSimpleName());
        punctuationCleanupStatement = esperAdministrator.createEPL(
            "on " + PunctuationCleanupEvent.class.getSimpleName() + " as punctuation " +
            "delete from LiveEventWindow as live " +
            "where live.timestamp >= punctuation.sliceStartTimestamp " +
            "and live.timestamp < punctuation.sliceEndTimestamp"
        );
    }

    /**
     * Sends a live event into Esper and acknowledges the tuple immediately.
     *
     * @param input Storm tuple from the live data stream.
     */
    private void processLiveEvent(Tuple input) {
        LiveEvent liveEvent = new LiveEvent(
            input.getLongByField(FIELD_ID),
            input.getLongByField(FIELD_TIMESTAMP),
            input.getDoubleByField(FIELD_VALUE),
            input.getIntegerByField(FIELD_PLUG_ID),
            input.getIntegerByField(FIELD_HOUSEHOLD_ID),
            input.getIntegerByField(FIELD_HOUSE_ID)
        );

        esperServiceProvider.getEPRuntime().sendEvent(liveEvent);
        collector.ack(input);
    }

    /**
     * Processes a punctuation, queries Esper for the current averages, emits
     * one tuple per plug, and then cleans up the processed slice from Esper.
     *
     * @param input Storm tuple from a punctuation stream.
     */
    private void processPunctuation(Tuple input) {
        int windowSizeMinutes = input.getIntegerByField(FIELD_WINDOW_SIZE);
        long sliceIndex = input.getLongByField(FIELD_SLICE_INDEX);

        if (windowSizeMinutes != this.windowSizeMinutes) {
            LOGGER.warn(
                "Ignoring punctuation for mismatched window: tupleWindow={}m boltWindow={}m",
                windowSizeMinutes,
                this.windowSizeMinutes
            );
            collector.ack(input);
            return;
        }

        long windowSizeSeconds = windowSizeMinutes * 60L;
        long sliceStartTimestamp = sliceIndex * windowSizeSeconds;
        long sliceEndTimestamp = sliceStartTimestamp + windowSizeSeconds;

        LOGGER.info("Received punctuation: window={}m sliceIndex={}", windowSizeMinutes, sliceIndex);

        long matchingEventCount = countMatchingEvents(sliceStartTimestamp, sliceEndTimestamp);
        int emittedPlugCount = emitCurrentAverages(input, windowSizeMinutes, sliceIndex, sliceStartTimestamp, sliceEndTimestamp);

        cleanupProcessedEvents(windowSizeMinutes, sliceIndex, sliceStartTimestamp, sliceEndTimestamp);

        LOGGER.info("Emitted {} plug averages for window={}m sliceIndex={}", emittedPlugCount, windowSizeMinutes, sliceIndex);
        LOGGER.info("Deleted {} processed events for window={}m sliceIndex={}", matchingEventCount, windowSizeMinutes, sliceIndex);

        collector.ack(input);
    }

    /**
     * Counts the number of live events that belong to the slice that is about to be processed.
     *
     * @param sliceStartTimestamp Inclusive slice start timestamp.
     * @param sliceEndTimestamp Exclusive slice end timestamp.
     * @return Number of matching live events.
     */
    private long countMatchingEvents(long sliceStartTimestamp, long sliceEndTimestamp) {
        EPStatement statement = null;

        try {
            statement = esperAdministrator.createEPL(
                "select * from LiveEventWindow where timestamp >= " + sliceStartTimestamp +
                " and timestamp < " + sliceEndTimestamp
            );

            long count = 0;
            Iterator<EventBean> iterator = statement.safeIterator();
            while (iterator.hasNext()) {
                iterator.next();
                count += 1;
            }

            return count;
        } finally {
            if (statement != null) {
                statement.destroy();
            }
        }
    }

    /**
     * Queries Esper for the average load per plug for the given slice and emits one tuple per group.
     *
     * @param windowSizeMinutes Window size in minutes.
     * @param sliceIndex Slice index reported by punctuation.
     * @param sliceStartTimestamp Inclusive slice start timestamp.
     * @param sliceEndTimestamp Exclusive slice end timestamp.
     * @return Number of plug groups emitted.
     */
    private int emitCurrentAverages(
        Tuple anchor,
        int windowSizeMinutes,
        long sliceIndex,
        long sliceStartTimestamp,
        long sliceEndTimestamp
    ) {
        EPStatement statement = null;
        int emittedCount = 0;

        try {
            statement = esperAdministrator.createEPL(
                "select houseId as houseId, householdId as householdId, plugId as plugId, avg(value) as currentAverage " +
                "from LiveEventWindow " +
                "where timestamp >= " + sliceStartTimestamp + " and timestamp < " + sliceEndTimestamp + " " +
                "group by houseId, householdId, plugId"
            );

            Iterator<EventBean> iterator = statement.safeIterator();
            while (iterator.hasNext()) {
                EventBean result = iterator.next();
                emitCurrentAverage(anchor, windowSizeMinutes, sliceIndex, result);
                emittedCount += 1;
            }

            return emittedCount;
        } finally {
            if (statement != null) {
                statement.destroy();
            }
        }
    }

    /**
     * Emits a single average result tuple to Storm.
     *
     * @param windowSizeMinutes Window size in minutes.
     * @param sliceIndex Slice index reported by punctuation.
     * @param result Esper result row.
     */
    private void emitCurrentAverage(Tuple anchor, int windowSizeMinutes, long sliceIndex, EventBean result) {
        int houseId = ((Number) result.get("houseId")).intValue();
        int householdId = ((Number) result.get("householdId")).intValue();
        int plugId = ((Number) result.get("plugId")).intValue();
        double currentAverage = ((Number) result.get("currentAverage")).doubleValue();

        collector.emit(
            "current-plug-average",
            anchor,
            new Values(windowSizeMinutes, sliceIndex, houseId, householdId, plugId, currentAverage)
        );
    }

    /**
     * Sends a cleanup event into Esper so the processed slice is removed from the named window.
     *
     * @param windowSizeMinutes Window size in minutes.
     * @param sliceIndex Slice index reported by punctuation.
     * @param sliceStartTimestamp Inclusive slice start timestamp.
     * @param sliceEndTimestamp Exclusive slice end timestamp.
     */
    private void cleanupProcessedEvents(
        int windowSizeMinutes,
        long sliceIndex,
        long sliceStartTimestamp,
        long sliceEndTimestamp
    ) {
        esperServiceProvider.getEPRuntime().sendEvent(
            new PunctuationCleanupEvent(windowSizeMinutes, sliceIndex, sliceStartTimestamp, sliceEndTimestamp)
        );
    }

    /**
     * Destroys Esper statements and tears down the runtime.
     */
    private void cleanupStatements() {
        if (liveEventIngestStatement != null) {
            liveEventIngestStatement.destroy();
            liveEventIngestStatement = null;
        }

        if (punctuationCleanupStatement != null) {
            punctuationCleanupStatement.destroy();
            punctuationCleanupStatement = null;
        }

        if (esperServiceProvider != null) {
            esperServiceProvider.destroy();
            esperServiceProvider = null;
            esperAdministrator = null;
        }
    }

    /**
     * Live event representation stored in Esper.
     */
    public static final class LiveEvent {

        private final long id;
        private final long timestamp;
        private final double value;
        private final int plugId;
        private final int householdId;
        private final int houseId;

        /**
         * Creates a live event object.
         */
        public LiveEvent(long id, long timestamp, double value, int plugId, int householdId, int houseId) {
            this.id = id;
            this.timestamp = timestamp;
            this.value = value;
            this.plugId = plugId;
            this.householdId = householdId;
            this.houseId = houseId;
        }

        public long getId() {
            return id;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getValue() {
            return value;
        }

        public int getPlugId() {
            return plugId;
        }

        public int getHouseholdId() {
            return householdId;
        }

        public int getHouseId() {
            return houseId;
        }
    }

    /**
     * Internal punctuation event used to remove processed events from Esper.
     */
    public static final class PunctuationCleanupEvent {

        private final int windowSize;
        private final long sliceIndex;
        private final long sliceStartTimestamp;
        private final long sliceEndTimestamp;

        /**
         * Creates a cleanup trigger for one punctuation slice.
         */
        public PunctuationCleanupEvent(
            int windowSize,
            long sliceIndex,
            long sliceStartTimestamp,
            long sliceEndTimestamp
        ) {
            this.windowSize = windowSize;
            this.sliceIndex = sliceIndex;
            this.sliceStartTimestamp = sliceStartTimestamp;
            this.sliceEndTimestamp = sliceEndTimestamp;
        }

        public int getWindowSize() {
            return windowSize;
        }

        public long getSliceIndex() {
            return sliceIndex;
        }

        public long getSliceStartTimestamp() {
            return sliceStartTimestamp;
        }

        public long getSliceEndTimestamp() {
            return sliceEndTimestamp;
        }
    }
}