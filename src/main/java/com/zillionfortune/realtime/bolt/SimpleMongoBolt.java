package com.zillionfortune.realtime.bolt;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import org.slf4j.LoggerFactory;

/**
 * A simple implementation of {@link MongoBolt} which attempts to map the input
 * tuple directly to a MongoDB object.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public class SimpleMongoBolt extends BaseBasicBolt  {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SimpleMongoBolt.class);


    private static final String HOST = "ip";
    private static final int PORT = 27017;  
    private static final String DB_NAME = "zjs";  
    private static final String COLLECTION = "zjs";
    // private static final String USER = "iwtxokhtd";  
    // private static final String PASSWORD = "123456";  
    private static Mongo conn=null;  
    private static DB myDB=null;  
    private static DBCollection myCollection=null;  
      
    static{  
        try {  
            conn=new Mongo(HOST,PORT);//建立数据库连接  
            myDB=conn.getDB(DB_NAME);//使用test数据库  
//            boolean loginSuccess=myDB.authenticate("", "");//用户验证  
//            if(loginSuccess){  
//                myCollection=myDB.getCollection(COLLECTION);  
//            }  
            myCollection=myDB.getCollection(COLLECTION);  
        } catch (UnknownHostException e) {  
            e.printStackTrace();  
        } catch (MongoException e) {  
            e.printStackTrace();  
        }  
    }  

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ywlog")); 
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String sentence = tuple.getValue(0).toString();
		try {
			sentence = new String(sentence.getBytes(),"utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
        collector.emit(new Values(sentence));
        List<DBObject> dbList = new ArrayList<DBObject>();  
        BasicDBObject doc1 = new BasicDBObject();  
        doc1.put("ywlog", sentence);
        int left = sentence.indexOf("datetime");
        if(sentence.contains("datetime")){
        	doc1.put("datetime", sentence.substring(left+11, left+30));
        }
        dbList.add(doc1); 
        myCollection.insert(dbList);
	}
}
