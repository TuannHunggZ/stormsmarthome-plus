package com.storm.iotdata;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.storm.iotdata.models.StormConfig;
import com.storm.iotdata.storm.*;

public class MainTopo {

    public static void main(String[] args) throws Exception {
        StormConfig stormConfig = new StormConfig();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-data", new Spout_data(stormConfig), 1);

        for (Integer timeSlice : stormConfig.getTimeSlicesMinutes()) {
            String boltId = "bolt-current-plug-average-" + timeSlice + "m";
            builder
                .setBolt(boltId, new Bolt_currentPlugAverage(stormConfig, timeSlice), 1)
                .fieldsGrouping("spout-data", "data", new Fields("houseId", "householdId", "plugId"))
                .allGrouping("spout-data", "punctuation-" + timeSlice + "m");
        }

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology(
                "iot-smarthome",
                config,
                builder.createTopology()
            );

            Thread.sleep(10000);
        } finally {
            cluster.close();
        }
    }
}