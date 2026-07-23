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
                new Bolt_average(stormConfig.getBoltAverageConfig(), windowSize),
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

        String plugAverageStreamId = stormConfig.getBoltAverageConfig().getOutputPlugStreamId();
        String houseAverageStreamId = stormConfig.getBoltAverageConfig().getOutputHouseStreamId();

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

        String punctuationStreamPrefix = stormConfig.getBoltPlugMedianConfig().getInputStreamIdPrefix();
        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
            medianBolt.allGrouping("spout-data", punctuationStreamPrefix + windowSize + "m");
        }

        BoltDeclarer houseMedianBolt = builder.setBolt(
            "bolt-house-median",
            new Bolt_houseMedian(stormConfig.getBoltHouseMedianConfig()),
            1
        );

        String housePunctuationStreamPrefix = stormConfig.getBoltHouseMedianConfig().getInputStreamIdPrefix();
        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
            houseMedianBolt.allGrouping("spout-data", housePunctuationStreamPrefix + windowSize + "m");
        }

        BoltDeclarer forecastBolt = builder.setBolt(
            "bolt-plug-forecast",
            new Bolt_plugForecast(stormConfig.getBoltPlugForecastConfig()),
            1
        );

        String inputAverageStreamId = stormConfig.getBoltPlugForecastConfig().getInputAverageStreamId();
        String inputMedianStreamId = stormConfig.getBoltPlugForecastConfig().getInputMedianStreamId();

        for (Integer windowSize : stormConfig.getTimeSlicesMinutes()) {
            forecastBolt.fieldsGrouping(
                "bolt-average-" + windowSize + "m",
                inputAverageStreamId,
                new Fields("windowSize", "sliceIndex", "houseId", "householdId", "plugId")
            );
        }

        forecastBolt.fieldsGrouping(
            "bolt-plug-median",
            inputMedianStreamId,
            new Fields("windowSize", "sliceIndex", "houseId", "householdId", "plugId")
        );

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);

        StormSubmitter.submitTopology("iot-smarthome", config, builder.createTopology());
    }
}