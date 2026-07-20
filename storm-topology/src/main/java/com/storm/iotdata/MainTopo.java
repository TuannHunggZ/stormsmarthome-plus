package com.storm.iotdata;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import com.storm.iotdata.models.StormConfig;
import com.storm.iotdata.storm.*;

public class MainTopo {

    public static void main(String[] args) throws Exception {
        StormConfig stormConfig = new StormConfig();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-data", new Spout_data(stormConfig.getSpoutDataConfig(), stormConfig.getTimeSlicesMinutes()), 1);

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