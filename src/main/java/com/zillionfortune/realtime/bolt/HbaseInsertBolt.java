package com.zillionfortune.realtime.bolt;

import java.io.IOException;
import java.io.Serializable;
import java.sql.BatchUpdateException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.zillionfortune.realtime.model.Ywlog;

import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.slf4j.LoggerFactory;


@SuppressWarnings("deprecation")
public class HbaseInsertBolt  extends BaseRichBolt {
	/**
	 * 管理hbase表
	 */
	private static HBaseAdmin admin;

	private static List<Row> logInfoBatch = new ArrayList<Row>();

	private static List<Tuple> logInfoTuple = new ArrayList<Tuple>();

	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TransformBolt.class);
	

    private OutputCollector collector;

	private MyTimer commitTimer = new MyTimer();
	
	/**
	 * 对表中的数据CRDU的对象
	 */
	private static HTable htable;
    static {  
    	Configuration config =HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5");
		try {
			admin =new HBaseAdmin(config);
			htable =new HTable(config, "ywlog2");
			htable.setWriteBufferSize(5 * 1024 * 1024); //5MB
			htable.setAutoFlush(false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }  
 /**   
    //获取陪着参数
    Configuration config = HBaseConfiguration.create();
    //建立连接
    Connection connection = ConnectionFactory.createConnection(config);
    try {
        //连接表 获取表对象
        Table t = connection.getTable(TableName.valueOf("testtable"));
        BufferedMutator table = connection.getBufferedMutator(TableName.valueOf("testtable"));
        try {
            Put p = new Put(Bytes.toBytes("myrow-1"));
            //p.add(); 这个地方的add 是个过期的方法然而我并不知道Cell的用法是什么
            p.add(Bytes.toBytes("colfam1"), Bytes.toBytes("name1"), Bytes.toBytes("zhangsan1"));
            //table.put(p);
            List<Mutation> mutations = new ArrayList<Mutation>();
            mutations.add(p);
            table.mutate(mutations);
            //如果不flush 在后面get可能是看不见的
            table.flush();
            // Close your table and cluster connection.
            Get get=new Get(Bytes.toBytes("myrow-1"));
            Result result=t.get(get);
            for(Cell cell:result.rawCells()){
                System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                System.out.print("\t列簇: "+new String(CellUtil.cloneFamily(cell)));
                System.out.print("\t列: "+new String(CellUtil.cloneQualifier(cell)));
                System.out.print("\t值: "+new String(CellUtil.cloneValue(cell)));
                System.out.println("\t时间戳: "+cell.getTimestamp());
            }
            System.out.print(">>>>end");
        } finally {
            if (table != null) table.close();
        }
    } finally {
        connection.close();
    }
    
    
    **/
    /** 
     * 插入数据
     * @throws IOException 
     * 	uuid     //uuid
		mobile    //手机号
		vd       //设备号
		os       //系统版本
		platform  //平台类型
		web       //浏览器版本
		vd_wh     //设备长宽
		channelCode   //渠道号
		appVd      //app版本号
		mobileType  //机型
		actionType   //点击行为编号
		messageContent   //消息内容
		messageType     //消息类型 
		ip         //ip
		logtime   //日志时间
     */  
    public static void insertData(Tuple input) throws IOException {
    	//uid+datetime+messagetype   15921952463_20160922132700_1_两位随机数字
		logInfoTuple.add(input);
		Ywlog log = (Ywlog) input.getValue(0);
    	StringBuffer rowkey = new StringBuffer();

    	Random r = new Random();
    	if(!StringUtils.isEmpty(log.getUuid())){
    		rowkey.append(log.getUuid()).append("_");
    	}else{
    		rowkey.append(log.getMobile()).append("_");
    	}
    	if(!StringUtils.isEmpty(log.getLogtime())){
    		rowkey.append(log.getLogtime().replace("-", "").replace(":", "").replace(" ", "")).append("_");
    	}
    	if(!StringUtils.isEmpty(log.getMessageType())){
    		rowkey.append(log.getMessageType()).append("_");
    	}
        rowkey.append(r.nextInt(100));

    	Put put =new Put(rowkey.toString().getBytes());
    	if(!StringUtils.isEmpty(log.getUuid())){
    		put.add("log".getBytes(), "uuid".getBytes(), log.getUuid().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getMobile())){
    		put.add("log".getBytes(), "mobile".getBytes(), log.getMobile().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getVd())){
    		put.add("log".getBytes(), "vd".getBytes(), log.getVd().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getOs())){
    		put.add("log".getBytes(), "os".getBytes(), log.getOs().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getPlatform())){
    		put.add("log".getBytes(), "platform".getBytes(), log.getPlatform().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getWeb())){
    		put.add("log".getBytes(), "web".getBytes(), log.getWeb().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getVd_wh())){
    		put.add("log".getBytes(), "vd_wh".getBytes(), log.getVd_wh().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getChannelCode())){
    		put.add("log".getBytes(), "channelCode".getBytes(), log.getChannelCode().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getAppVd())){
    		put.add("log".getBytes(), "appVd".getBytes(), log.getAppVd().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getMobileType())){
    		put.add("log".getBytes(), "mobileType".getBytes(), log.getMobileType().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getActionType())){
    		put.add("log".getBytes(), "actionType".getBytes(), log.getActionType().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getMessageContent())){
    		put.add("log".getBytes(), "messageContent".getBytes(), log.getMessageContent().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getMessageType())){
    		put.add("log".getBytes(), "messageType".getBytes(), log.getMessageType().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getIp())){
    		put.add("log".getBytes(), "ip".getBytes(), log.getIp().getBytes());
    	}
    	if(!StringUtils.isEmpty(log.getLogtime())){
    		put.add("log".getBytes(), "logtime".getBytes(), log.getLogtime().getBytes());
    	}
		logInfoBatch.add(put);
    	//put.add("log".getBytes(), "test".getBytes(), "2111111111".getBytes());
//		htable.put(put);
    } 
      
    
   
	/*@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Ywlog ywlog = new Ywlog();
		ywlog = (Ywlog) tuple.getValue(0);
        try {
			insertData("ywlog",ywlog);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		//不再往下继续传了
	}

	public static void main(String[] args) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse("2008-08-08 12:10:12");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
		String a = sdf2.format(date);
		LOG.info(a);
	}



	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		commitTimer.schedule(new CommitTimerTask(), 5 * 1000);

	}



	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		try {
			insertData(input);
		}catch (IOException e) {
			e.printStackTrace();
			collector.fail(input);
		}


		try {
			if(logInfoBatch.size() >= 100) {
				Object[] result = new Object[logInfoBatch.size()];
				htable.batch(logInfoBatch, result);
				for(int i = 0 ; i <= logInfoTuple.size(); i ++){
					collector.ack(logInfoTuple.get(i));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			for(int i = 0 ; i <= logInfoTuple.size(); i ++){
				collector.fail(logInfoTuple.get(i));
			}
		} catch (InterruptedException e) {
			for(int i = 0 ; i <= logInfoTuple.size(); i ++){
				collector.fail(logInfoTuple.get(i));
			}
		}


	}

	private class CommitTimerTask extends TimerTask implements Serializable {

		public void run() {
			LOG.info("Commit rows because auto interval commit task. Row count committing : " + logInfoBatch.size());
			Object[] result = new Object[logInfoBatch.size()];
			try {
				htable.batch(logInfoBatch, result);
				for(int i = 0 ; i <= logInfoTuple.size(); i ++){
					collector.ack(logInfoTuple.get(i));
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				for(int i = 0 ; i <= logInfoTuple.size(); i ++){
					collector.fail(logInfoTuple.get(i));
				}
			} catch (IOException e) {
				e.printStackTrace();
				for(int i = 0 ; i <= logInfoTuple.size(); i ++){
					collector.fail(logInfoTuple.get(i));
				}
			}
		}
	}

}


class MyTimer extends Timer implements Serializable {
}