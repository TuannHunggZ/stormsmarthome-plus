package com.storm.iotdata;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.storm.iotdata.models.StormConfig;
import com.storm.iotdata.storm.*;

public class MainTopo {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-data", new Spout_data(), 1);

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            String averageBoltId = "bolt-average-" + windowSize + "m";
            BoltDeclarer boltDeclarer = builder.setBolt(
                averageBoltId,
                new Bolt_average(windowSize),
                2
            );

            boltDeclarer.fieldsGrouping(
                "spout-data",
                "data",
                new Fields("houseId")
            );
            boltDeclarer.allGrouping("spout-data", "punctuation-" + windowSize + "m");
        }

        BoltDeclarer plugPersistenceBolt = builder.setBolt(
            "bolt-plug-average-persistence",
            new Bolt_plugAveragePersistence(),
            1
        );
        BoltDeclarer housePersistenceBolt = builder.setBolt(
            "bolt-house-average-persistence",
            new Bolt_houseAveragePersistence(),
            1
        );

        String plugAverageStreamId = "current-plug-average";
        String houseAverageStreamId = "current-house-average";

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            String averageBoltId = "bolt-average-" + windowSize + "m";
            String punctuationStreamId = "punctuation-" + windowSize + "m";
            plugPersistenceBolt.shuffleGrouping(averageBoltId, plugAverageStreamId);
            plugPersistenceBolt.allGrouping(averageBoltId, punctuationStreamId);
            housePersistenceBolt.shuffleGrouping(averageBoltId, houseAverageStreamId);
            housePersistenceBolt.allGrouping(averageBoltId, punctuationStreamId);
        }

        BoltDeclarer anomalyDetectionBolt = builder.setBolt(
            "bolt-plug-anomaly-detection",
            new Bolt_plugAnomalyDetection(),
            1
        );

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            anomalyDetectionBolt.fieldsGrouping(
                "bolt-average-" + windowSize + "m",
                plugAverageStreamId,
                new Fields("windowSize", "houseId", "householdId", "plugId")
            );
        }

        BoltDeclarer houseAnomalyDetectionBolt = builder.setBolt(
            "bolt-house-anomaly-detection",
            new Bolt_houseAnomalyDetection(),
            1
        );

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            houseAnomalyDetectionBolt.fieldsGrouping(
                "bolt-average-" + windowSize + "m",
                houseAverageStreamId,
                new Fields("windowSize", "houseId")
            );
        }

        BoltDeclarer houseForecastBolt = builder.setBolt(
            "bolt-house-forecast",
            new Bolt_houseForecast(),
            2
        );

        String houseForecastAverageStreamId = "current-house-average";

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            houseForecastBolt.shuffleGrouping(
                "bolt-average-" + windowSize + "m",
                houseForecastAverageStreamId
            );
            houseForecastBolt.allGrouping(
                "bolt-average-" + windowSize + "m",
                "punctuation-" + windowSize + "m"
            );
        }

        BoltDeclarer forecastBolt = builder.setBolt(
            "bolt-plug-forecast",
            new Bolt_plugForecast(),
            2
        );

        String inputAverageStreamId = "current-plug-average";

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            forecastBolt.shuffleGrouping(
                "bolt-average-" + windowSize + "m",
                inputAverageStreamId
            );
            forecastBolt.allGrouping(
                "bolt-average-" + windowSize + "m",
                "punctuation-" + windowSize + "m"
            );
        }

        BoltDeclarer medianBolt = builder.setBolt(
            "bolt-plug-median",
            new Bolt_plugMedian(),
            1
        );
        medianBolt.allGrouping(
            "bolt-plug-average-persistence",
            "plug-average-persisted"
        );
        medianBolt.allGrouping(
            "bolt-plug-forecast",
            "plug-forecast-persisted"
        );

        BoltDeclarer houseMedianBolt = builder.setBolt(
            "bolt-house-median",
            new Bolt_houseMedian(),
            1
        );
        houseMedianBolt.allGrouping(
            "bolt-house-average-persistence",
            "house-average-persisted"
        );
        houseMedianBolt.allGrouping(
            "bolt-house-forecast",
            "house-forecast-persisted"
        );

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);

        StormSubmitter.submitTopology("iot-smarthome", config, builder.createTopology());
    }
}