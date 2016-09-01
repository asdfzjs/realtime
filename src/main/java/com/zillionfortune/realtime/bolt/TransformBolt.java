package com.zillionfortune.realtime.bolt;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TransformBolt extends BaseRichBolt{
	
	 private static final Log LOG = LogFactory.getLog(TransformBolt.class);
     private OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	//格式化日志
	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);//获取到每行日志
		System.out.println("zjs:"+line);
		collector.emit(input, new Values(line,1));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log", "count"));     
	}

}
