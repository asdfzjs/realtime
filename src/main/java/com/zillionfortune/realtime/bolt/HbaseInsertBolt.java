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
//	static HBaseConfiguration hbaseConfig=null;  
//    static{  
//        Configuration config=new Configuration();  
//        config.set("hbase.zookeeper.quorum","node1,node2,node3,node4,node5");  
//        config.set("hbase.zookeeper.property.clientPort", "2181");  
//        hbaseConfig=new HBaseConfiguration(config);  
//    } 
	public static Configuration configuration;  
    static {  
        configuration = HBaseConfiguration.create();  
        configuration.set("hbase.zookeeper.property.clientPort", "2181");  
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5");  
        configuration.set("hbase.master", "node1:6000");  
    }  
    
      
    /** 
     * 插入数据 
     * @param tableName 
     */  
    public static void insertData(String tableName,String value) {  
        System.out.println("start insert data ......");  
        HTablePool pool = new HTablePool(configuration, 1000);  
        HTable table = (HTable) pool.getTable(tableName);  
        Put put = new Put(value.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值  
        put.add("info".getBytes(), null, "user".getBytes());// 本行数据的第一列  
//        put.add("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列  
//        put.add("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列  
        try {  
            table.put(put);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        System.out.println("end insert data ......");  
    } 
      
      
   
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String sentence = (String) tuple.getValue(0);  
        String out = sentence;  
        collector.emit(new Values(out));
        insertData("user",new Values(out).toString());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}


}
