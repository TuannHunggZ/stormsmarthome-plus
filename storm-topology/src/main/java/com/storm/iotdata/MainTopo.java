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
        StormConfig stormConfig = new StormConfig();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-data", new Spout_data(stormConfig.getSpoutDataConfig(), stormConfig.getTimeSlicesMinutes()), 1);

        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
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
            new Bolt_averagePersistence(stormConfig.getBoltAveragePersistenceConfig()),
            1
        );

        String plugAverageStreamId = "current-plug-average";
        String houseAverageStreamId = "current-house-average";

        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
            String boltId = "bolt-average-" + windowSize + "m";
            persistenceBolt.allGrouping(boltId, plugAverageStreamId);
            persistenceBolt.allGrouping(boltId, houseAverageStreamId);
        }

        BoltDeclarer medianBolt = builder.setBolt(
            "bolt-plug-median",
            new Bolt_plugMedian(stormConfig.getBoltPlugMedianConfig()),
            1
        );

        String punctuationStreamPrefix = "punctuation-";
        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
            medianBolt.allGrouping("spout-data", punctuationStreamPrefix + windowSize + "m");
        }

        BoltDeclarer houseMedianBolt = builder.setBolt(
            "bolt-house-median",
            new Bolt_houseMedian(stormConfig.getBoltHouseMedianConfig()),
            1
        );

        String housePunctuationStreamPrefix = "punctuation-";
        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
            houseMedianBolt.allGrouping("spout-data", housePunctuationStreamPrefix + windowSize + "m");
        }

        BoltDeclarer houseForecastBolt = builder.setBolt(
            "bolt-house-forecast",
            new Bolt_houseForecast(stormConfig.getBoltHouseForecastConfig()),
            1
        );

        String houseForecastAverageStreamId = "current-house-average";
        String houseForecastMedianStreamId = "archive-house-median";

        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
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
            new Bolt_plugForecast(stormConfig.getBoltPlugForecastConfig()),
            1
        );

        String inputAverageStreamId = "current-plug-average";
        String inputMedianStreamId = "archive-plug-median";

        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
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