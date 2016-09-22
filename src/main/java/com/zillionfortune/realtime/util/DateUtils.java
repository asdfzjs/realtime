package com.zillionfortune.realtime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DateUtils {
	//String to date
	public Date parseDate(String a) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(a);
		return date;
	}
	
	public static void main(String[] args) {
		String a = "2008-08-08 12:10:12";
		System.out.println(a.replace("-", "").replace(":", "").replace(" ", ""));
	}
}
