package com.zillionfortune.realtime.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zillionfortune.realtime.bolt.TransformBolt;
import com.zillionfortune.realtime.model.Ywlog;

import clojure.main;
import net.sf.json.JSONObject;

/*
点击日志：
{
    "d": {
        "p": "1.2.x",
        "u": "15827060231",
        "m": "微信分享失败"
    },
    "t": 2,
    "ip": "127.0.0.1",
    "datetime": "2016-09-21 16:07:00"
}

基础信息：
{
    "d": {
        "u": "15827060231",
        "d": "*******",
        "o": "iPhone OS",
        "p": 2,
        "w": "safari 9",
        "s": "411,736",
        "c": "wandoujia",
        "v": "1.0.5",
        "t": "iphone6 plus"
    },
    "t": 1,
    "ip": "127.0.0.1",
    "datetime": "2016-09-21 16:07:00"
}

存留事件：
{
    "d": {
        "p": "1.2.x",
        "u": "15827060231",
        "m": "微信分享失败"
    },
    "t": 3,
    "ip": "127.0.0.1",
    "datetime": "2016-09-21 16:07:00"
}
*/
public class YwlogParse {
	//private static Logger LOG = LoggerFactory.getLogger(YwlogParse.class);
	//parse 日志
	public static Ywlog logParse(String line){
		Ywlog ywlog = new Ywlog();
		ywlog.setParse(true);
		try{
			JSONObject obj = JSONObject.fromObject(line);
			ywlog.setIp(obj.getString("ip"));
			ywlog.setLogtime(obj.getString("datetime"));
			ywlog.setMessageType(obj.getString("t"));
			String dcontent = obj.getString("d");
			JSONObject obj2 = JSONObject.fromObject(dcontent);
			if(obj2.getString("u").length()>11){//大于11位就是uuid
				ywlog.setUuid(obj2.getString("u"));
			}else{
				ywlog.setMobile(obj2.getString("u"));
			}
			if(obj.getString("t").equals("1")){//基础数据
				if(obj2.has("d")){
					ywlog.setVd(obj2.getString("d"));
				}
				if(obj2.has("o")){
					ywlog.setOs(obj2.getString("o"));
				}
				if(obj2.has("p")){
					ywlog.setPlatform(obj2.getString("p"));
				}
				if(obj2.has("w")){
					ywlog.setWeb(obj2.getString("w"));
				}
				if(obj2.has("s")){
					ywlog.setVd_wh(obj2.getString("s"));
				}
				if(obj2.has("c")){
					ywlog.setChannelCode(obj2.getString("c"));
				}
				if(obj2.has("v")){
					ywlog.setAppVd(obj2.getString("v"));
				}
				if(obj2.has("t")){
					ywlog.setMobileType(obj2.getString("t"));
				}
				
			}else if(obj.getString("t").equals("2")){//点击数据
				if(obj2.has("p")){
					ywlog.setActionType(obj2.getString("p"));
				}
				if(obj2.has("m")){
					ywlog.setMessageContent(obj2.getString("m"));
				}
			}else if(obj.getString("t").equals("3")){
				if(obj2.has("p")){
					ywlog.setActionType(obj2.getString("p"));
				}
				if(obj2.has("m")){
					ywlog.setMessageContent(obj2.getString("m"));
				}
			}
		}catch(net.sf.json.JSONException e){
			ywlog.setParse(false);
		}
		return ywlog;
	}
	
	public static void main(String[] args) {
		//String line = "{\"d\": {\"u\": \"15827060231\",\"d\": \"*******\",\"o\": \"iPhone OS\",\"p\": 2,\"w\": \"safari 9\",\"s\": \"411,736\",\"c\": \"wandoujia\",\"v\": \"1.0.5\",\"t\": \"iphone6 plus\"},\"t\": 1,\"ip\": \"127.0.0.1\",\"datetime\": \"2016-09-21 16:07:00\"}";
		String line = "{\"d\":{\"c\":\"yingyongbao\",\"d\":\"353771072601791\",\"o\":\"5.1\",\"p\":1,\"s\":\"1280,720\",\"t\":\"SM-J5008\",\"u\":\"13680607099\",\"v\":\"2.2.1\"},\"t\":1,\"datetime\":\"2016-10-10 17:02:52\",\"ip\":\"223.73.100.205\"}";
	//	int left = line.indexOf("datetime");
		//		JSONObject obj = JSONObject.fromObject(line);
		Ywlog ywlog = YwlogParse.logParse(line);
//		ywlog.setIp(obj.getString("ip"));
//		ywlog.setLogtime(obj.getString("datetime"));
//		ywlog.setMessageType(obj.getString("t"));
//		String dcontent = obj.getString("d");
//		JSONObject obj2 = JSONObject.fromObject(dcontent);
//		if(obj2.get("u").toString().length()>11){//大于11位就是uuid
//			ywlog.setUuid(obj2.getString("u"));
//		}else{
//			ywlog.setMobile(obj2.getString("u"));
//		}
//		if(obj.get("t").equals("1")){//基础数据
//			ywlog.setVd(obj2.getString("d"));
//			ywlog.setOs(obj2.getString("o"));
//			ywlog.setPlatform(obj2.getString("p"));
//			ywlog.setWeb(obj2.getString("w"));
//			ywlog.setVd_wh(obj2.getString("s"));
//			ywlog.setChannelCode(obj2.getString("c"));
//			ywlog.setAppVd(obj2.getString("v"));
//			ywlog.setMobileType(obj2.getString("t"));
//			
//		}else if(obj.getString("t").equals("2")){//点击数据
//			ywlog.setActionType(obj2.getString("p"));
//			ywlog.setMessageContent(obj2.getString("m"));
//		}
	}
}
