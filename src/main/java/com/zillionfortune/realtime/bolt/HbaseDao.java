package com.zillionfortune.realtime.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseDao {

	/**
	 * 管理hbase表
	 */
	private HBaseAdmin admin;
	
	/**
	 * 对表中的数据CRDU的对象
	 */
	private HTable htable;
	
	@Before
	public void setup()throws Exception{
		Configuration config =HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "node1,node2,node3");
//		admin =new HBaseAdmin(config);
		htable =new HTable(config, "t_test");
	}
	
	@After
	public void end()throws Exception{
		if(htable!=null){
			htable.close();
		}
	}
	
	@Test
	public void createTable()throws Exception{
		HTableDescriptor table =new HTableDescriptor(TableName.valueOf("t_test"));
		HColumnDescriptor cf1 =new HColumnDescriptor("cf1".getBytes());
		HColumnDescriptor cf2 =new HColumnDescriptor("cf2".getBytes());
		cf1.setMaxVersions(3);
		cf1.setMinVersions(1);
		table.addFamily(cf1);
		table.addFamily(cf2);
		
		if(admin.tableExists("t_test".getBytes())){
			//删除表
			admin.disableTable("t_test");
			admin.deleteTable("t_test");
		}
		admin.createTable(table);
		admin.close();
	}
	
	@Test
	public void insertData()throws Exception{
		//新增一个单元格
//		Put put =new Put("001".getBytes());
//		put.add("cf1".getBytes(), "name".getBytes(), "张三".getBytes());
//		htable.put(put);
		//新增一行
		Put put =new Put("001".getBytes());
		put.add("cf1".getBytes(), "age".getBytes(), "24".getBytes());
		put.add("cf1".getBytes(), "sex".getBytes(), "男".getBytes());
		htable.put(put);
		
		//新增多行
//		htable.put(list);
	}
	
	@Test
	public void getData()throws Exception{
		//查一行
		Get get =new Get("001".getBytes());
		get.setMaxVersions(1);//返回结果中只取最新的版本
//		get.addColumn("cf1".getBytes(), "sex".getBytes());删除则查询一行
		Result res= htable.get(get);//一个result表示一行
		for( Cell c :res.listCells()){
			System.out.println( new String(c.getValue()));
		}
	}
	
	//多行查询
	@Test
	public void findDatas()throws Exception{
		Scan s =new Scan();//整表遍历
		s.setStartRow("000".getBytes());//范围遍历表
		s.setStopRow("005".getBytes());
		ResultScanner rss= htable.getScanner(s);
		for( Result res : rss){
			for( Cell c :res.listCells()){
//				System.out.println( new String(c.getValue()));
				System.out.println(new String(CellUtil.cloneValue(c)));
				System.out.println(new String(CellUtil.cloneRow(c)));
			}
		}
	}
	
	@Test
	public void deleteData()throws Exception{
		//删除一个单元格
		Delete d =new Delete("001".getBytes());
//		d.deleteColumns("cf1".getBytes(), "sex".getBytes());不加则删除整行
		htable.delete(d);
		//删除整行
	}
	
	
}
