package com.zillionfortune.realtime.bolt;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * A simple implementation of {@link MongoBolt} which attempts to map the input
 * tuple directly to a MongoDB object.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public class SimpleMongoBolt extends BaseBasicBolt  {
	private static final String HOST = "192.168.210.66";  
    private static final int PORT = 27017;  
   // private static final String USER = "iwtxokhtd";  
   // private static final String PASSWORD = "123456";  
    private static final String DB_NAME = "zjs";  
    private static final String COLLECTION = "zjs";  
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

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) { }



	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
	}


	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String sentence = (String) tuple.getValue(0);  
        String out = sentence + "!";  
        collector.emit(new Values(out));
	}
}
