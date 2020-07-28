package com.Hadoop.Service;

import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;  


@Service
public class HbaseService {
	
	public String  SearchTable(String tablename,String rowkey,String zkip,String zkport) {
		try {
		if(rowkey.equals("")) {
			return SearchAllTable(tablename,zkip,zkport);
		}else {
			return SearchTableRowkey(tablename,rowkey,zkip,zkport);
		}}catch (Exception e) {
			return e.getMessage();
		}
		
	}
 
	
	private String SearchAllTable(String tablename,String zkip,String zkport) throws IOException { 
		Table table=initHbase(zkip,zkport).getTable(TableName.valueOf(tablename)); 
		 Scan scan = new Scan();
		 ResultScanner scanner = table.getScanner(scan);
		 Map<String,Map<String,String>> result=new LinkedMap();
		 for (Result r :scanner){
			 r.listCells().forEach(cell -> {
		    	 if(result.containsKey(Bytes.toString(CellUtil.cloneRow(cell)))){
		    		 result.get(Bytes.toString(CellUtil.cloneRow(cell))).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
		    	 }else{
		    		 Map<String,String> map=new LinkedMap();
		    		 map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
		    		 result.put(Bytes.toString(CellUtil.cloneRow(cell)), map);
		    	 }
	               });
	        }
		 return JSON.toJSONString(result);
	}
	
	
	private String SearchTableRowkey(String tablename,String rowkey,String zkip,String zkport) throws IOException {
		 Table table=initHbase(zkip,zkport).getTable(TableName.valueOf(tablename)); 
		 Get g = new Get(Bytes.toBytes(rowkey));
		 Result r=table.get(g); 
		 Map<String,Map<String,String>> result=new LinkedMap();
	     r.listCells().forEach(cell -> {
	    	 if(result.containsKey(Bytes.toString(CellUtil.cloneRow(cell)))){
	    		 result.get(Bytes.toString(CellUtil.cloneRow(cell))).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
	    	 }else{
	    		 Map<String,String> map=new LinkedMap();
	    		 map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
	    		 result.put(Bytes.toString(CellUtil.cloneRow(cell)), map);
	    	 }
               });
      
		return   JSON.toJSONString(result);
	}
	
	private  Connection initHbase(String zkip,String zkport) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zkport);
        configuration.set("hbase.zookeeper.quorum", zkip);
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

}
