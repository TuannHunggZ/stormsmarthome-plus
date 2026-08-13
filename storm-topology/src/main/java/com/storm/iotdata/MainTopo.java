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
            String boltId = "bolt-average-" + windowSize + "m";
            BoltDeclarer boltDeclarer = builder.setBolt(
                boltId,
                new Bolt_average(windowSize),
                1
            );

            boltDeclarer.fieldsGrouping(
                "spout-data",
                "data",
                new Fields("houseId")
            );
            boltDeclarer.allGrouping("spout-data", "punctuation-" + windowSize + "m");
        }

        BoltDeclarer persistenceBolt = builder.setBolt(
            "bolt-average-persistence",
            new Bolt_averagePersistence(),
            1
        );

        String plugAverageStreamId = "current-plug-average";
        String houseAverageStreamId = "current-house-average";

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            String boltId = "bolt-average-" + windowSize + "m";
            persistenceBolt.allGrouping(boltId, plugAverageStreamId);
            persistenceBolt.allGrouping(boltId, houseAverageStreamId);
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

        BoltDeclarer medianBolt = builder.setBolt(
            "bolt-plug-median",
            new Bolt_plugMedian(),
            1
        );

        String punctuationStreamPrefix = "punctuation-";
        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            medianBolt.allGrouping("spout-data", punctuationStreamPrefix + windowSize + "m");
        }

        BoltDeclarer houseMedianBolt = builder.setBolt(
            "bolt-house-median",
            new Bolt_houseMedian(),
            1
        );

        String housePunctuationStreamPrefix = "punctuation-";
        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            houseMedianBolt.allGrouping("spout-data", housePunctuationStreamPrefix + windowSize + "m");
        }

        BoltDeclarer houseForecastBolt = builder.setBolt(
            "bolt-house-forecast",
            new Bolt_houseForecast(),
            1
        );

        String houseForecastAverageStreamId = "current-house-average";
        String houseForecastMedianStreamId = "archive-house-median";

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            houseForecastBolt.fieldsGrouping(
                "bolt-average-" + windowSize + "m",
                houseForecastAverageStreamId,
                new Fields("windowSize", "timestamp", "houseId")
            );
        }

        houseForecastBolt.fieldsGrouping(
            "bolt-house-median",
            houseForecastMedianStreamId,
            new Fields("windowSize", "timestamp", "houseId")
        );

        BoltDeclarer forecastBolt = builder.setBolt(
            "bolt-plug-forecast",
            new Bolt_plugForecast(),
            1
        );

        String inputAverageStreamId = "current-plug-average";
        String inputMedianStreamId = "archive-plug-median";

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            forecastBolt.fieldsGrouping(
                "bolt-average-" + windowSize + "m",
                inputAverageStreamId,
                new Fields("windowSize", "timestamp", "houseId", "householdId", "plugId")
            );
        }

        forecastBolt.fieldsGrouping(
            "bolt-plug-median",
            inputMedianStreamId,
            new Fields("windowSize", "timestamp", "houseId", "householdId", "plugId")
        );

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);

        StormSubmitter.submitTopology("iot-smarthome", config, builder.createTopology());
    }
}