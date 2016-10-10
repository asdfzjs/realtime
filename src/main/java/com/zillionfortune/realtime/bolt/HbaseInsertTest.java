package com.zillionfortune.realtime.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class HbaseInsertTest {  
    static HBaseConfiguration hbaseConfig;  
  
    public static void main(String[] args) throws Exception {  
        Configuration HBASE_CONFIG = new Configuration();  
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "gms5,gms6");  
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");  
        hbaseConfig = new HBaseConfiguration(HBASE_CONFIG);  
        insert(false,false,1024*1024*24);  
        insert(false,true,0);  
        insert(true,true,0);  
    }  
  
    public static void insert(boolean wal,boolean autoFlush,long writeBuffer)  
            throws IOException {  
        String tableName="etltest";  
        HBaseAdmin hAdmin = new HBaseAdmin(hbaseConfig);  
        if (hAdmin.tableExists(tableName)) {  
            hAdmin.disableTable(tableName);  
            hAdmin.deleteTable(tableName);  
            HTableDescriptor t = new HTableDescriptor(tableName);  
            t.addFamily(new HColumnDescriptor("f1"));  
            t.addFamily(new HColumnDescriptor("f2"));  
            t.addFamily(new HColumnDescriptor("f3"));  
            t.addFamily(new HColumnDescriptor("f4"));  
            hAdmin.createTable(t);  
            System.out.println("table created");  
        }  
        HTable table = new HTable(hbaseConfig, tableName);  
        table.setAutoFlush(autoFlush);  
        if(writeBuffer!=0){  
            table.setWriteBufferSize(writeBuffer);  
        }  
        List<Put> lp = new ArrayList<Put>();  
        long all = System.currentTimeMillis();  
        int count = 10000;  
        byte[] buffer = new byte[1024];  
        Random r = new Random();  
        for (int i = 1; i <= count; ++i) {  
            Put p = new Put(String.format("row d",i).getBytes());  
            r.nextBytes(buffer);  
            p.add("f1".getBytes(), null, buffer);  
            p.add("f2".getBytes(), null, buffer);  
            p.add("f3".getBytes(), null, buffer);  
            p.add("f4".getBytes(), null, buffer);  
            p.setWriteToWAL(wal);  
            lp.add(p);  
            if(i==100){  
                table.put(lp);  
                lp.clear();  
            }  
        }  
        System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer);  
        System.out.println("insert complete"+",costs:"+(System.currentTimeMillis()-all)*1.0/count+"ms");  
    }  
}  