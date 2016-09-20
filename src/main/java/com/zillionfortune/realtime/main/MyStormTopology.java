package com.zillionfortune.realtime.main;




import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.zillionfortune.realtime.bolt.HbaseInsertBolt;
import com.zillionfortune.realtime.bolt.SimpleMongoBolt;
import com.zillionfortune.realtime.bolt.TransformBolt;


import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class MyStormTopology {

     public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {
          String zks = "node1:2181,node2:2181,node3:2181,node4:2181,node5:2181/kafka";
          String topic = "zjs";
          String zkRoot = "/storm"; // default zookeeper root configuration for storm
          String id = "word";
          BrokerHosts brokerHosts = new ZkHosts(zks);
          SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
          spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
          //spoutConf.forceFromStart = false;
          spoutConf.zkServers = Arrays.asList(new String[] {"node1", "node2", "node3","node4","node5"});
          spoutConf.zkPort = 2181;
          TopologyBuilder builder = new TopologyBuilder();
          builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); 
          builder.setBolt("word-splitter", new TransformBolt(), 2).shuffleGrouping("kafka-reader");
          builder.setBolt("mongo-insert", new SimpleMongoBolt(), 2).shuffleGrouping("word-splitter");
          //builder.setBolt("hbase-insert", new HbaseInsertBolt()).shuffleGrouping("mongo-insert");

          
        Config conf = new Config();
        String name = MyStormTopology.class.getSimpleName();
  		if (args != null && args.length > 0) {
  			conf.put(Config.NIMBUS_HOST, args[0]);
  			conf.setNumWorkers(3);
  			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  		} else {
  			LocalCluster cluster = new LocalCluster();
  			cluster.submitTopology(name, conf, builder.createTopology());
  		}
     }
}