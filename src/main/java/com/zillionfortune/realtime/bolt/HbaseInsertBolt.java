package com.zillionfortune.realtime.bolt;

import java.io.IOException;
import java.sql.BatchUpdateException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.hadoop.hbase.io.*;  

@SuppressWarnings("deprecation")
public class HbaseInsertBolt  extends BaseBasicBolt {
	/**
	 * 管理hbase表
	 */
	private static HBaseAdmin admin;
	
	/**
	 * 对表中的数据CRDU的对象
	 */
	private static HTable htable;
    static {  
    	Configuration config =HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5");
		try {
			admin =new HBaseAdmin(config);
			htable =new HTable(config, "user");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }  
    
      
    /** 
     * 插入数据 
     * @param tableName 
     * @throws IOException 
     */  
    public static void insertData(String tableName,String value) throws IOException {  
    	long rowkey = System.currentTimeMillis();
    	Put put =new Put(Long.toString(rowkey).getBytes());
		put.add("info".getBytes(), "name".getBytes(), value.getBytes());
		htable.put(put);
    } 
      
      
   
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
	//	String sentence = (String) tuple.getValue(0);  
//        String out = sentence;  
//        collector.emit(new Values(out));
        try {
			insertData("user",new Values(tuple.getValue(0)).toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}


}
