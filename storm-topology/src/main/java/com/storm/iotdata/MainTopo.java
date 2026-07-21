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
            String boltId = "bolt-current-plug-average-" + windowSize + "m";
            BoltDeclarer boltDeclarer = builder.setBolt(
                boltId,
                new Bolt_currentPlugAverage(stormConfig.getBoltCurrentPlugAverageConfig(), windowSize),
                1
            );

            boltDeclarer.fieldsGrouping(
                "spout-data",
                "data",
                new Fields("houseId", "householdId", "plugId")
            );
            boltDeclarer.allGrouping("spout-data", "punctuation-" + windowSize + "m");
        }

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);

        StormSubmitter.submitTopology("iot-smarthome", config, builder.createTopology());
    }
}