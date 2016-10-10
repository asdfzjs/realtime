package com.zillionfortune.realtime.bolt;

import java.io.UnsupportedEncodingException;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.zillionfortune.realtime.model.Ywlog;
import com.zillionfortune.realtime.util.YwlogParse;
import org.slf4j.LoggerFactory;

public class TransformBolt extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TransformBolt.class);


	// private static Logger LOG = LoggerFactory.getLogger(TransformBolt.class);
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		//过滤垃圾日志
		//String line = new String((byte[]) tuple.getValue(0), "UTF-8");
		String line = tuple.getValue(0).toString();
		try {
			line = new String(line.getBytes(),"utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		//过滤规则， 不是"{"开头的都不去做parse
		if(!line.startsWith("{")){
			//什么也不处理
			LOG.info("Log format content[SKIPPED]:" + line);
		}else{
			//logParse
			YwlogParse parse = new YwlogParse();
			Ywlog logmodel = parse.logParse(line);
			//发送ywlog对象
			if(logmodel.getParse()){
				collector.emit(new Values(logmodel));
			}else{//parse异常的
				LOG.info("Log parse content[SKIPPED]:" + line);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ywlog")); 
	}
	

	

}
