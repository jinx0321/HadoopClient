package com.Hadoop.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;  
import com.Hadoop.Log.*;

@Service
public class HbaseService {
	@Autowired
	Info Info;
	
	
	/**
	 * 查找hbase表中数据返回json
	 * @param tablename
	 * @param rowkey
	 * @param zkip
	 * @param zkport
	 * @return
	 */
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
		Connection conn = null;
		try {
		conn=initHbase(zkip,zkport);
		Table table=conn.getTable(TableName.valueOf(tablename)); 
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
		}catch(Exception e) {
			return e.getMessage();
		}finally {
			conn.close();
			
		}
		
	}
	
	
	private String SearchTableRowkey(String tablename,String rowkey,String zkip,String zkport) throws IOException {
		
		Connection conn = null;
		try {
	
		conn=initHbase(zkip,zkport);
		Table table=conn.getTable(TableName.valueOf(tablename));
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(rowkey));
		Scan scan = new Scan();
		scan.setFilter(rowFilter);
		ResultScanner scanner = table.getScanner(scan);
		 Map<String,Map<String,String>> result=new LinkedMap();
		 scanner.forEach(r->{
	     r.listCells().forEach(cell -> {
	    	 if(result.containsKey(Bytes.toString(CellUtil.cloneRow(cell)))){
	    		 result.get(Bytes.toString(CellUtil.cloneRow(cell))).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
	    	 }else{
	    		 Map<String,String> map=new LinkedMap();
	    		 map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
	    		 result.put(Bytes.toString(CellUtil.cloneRow(cell)), map);
	    	 }
               });
		 });
			return JSON.toJSONString(result);
			}catch(Exception e) {
				return e.getMessage();
			}finally {
				conn.close();
				
			}
	}
	
	
	public String InsertTable(String tablename,String rowkey,String zkip,String zkport,JSONArray jr) {
		Connection conn = null;
		try {
		    conn=initHbase(zkip,zkport);
			Table table=initHbase(zkip,zkport).getTable(TableName.valueOf(tablename));
			Put put=new Put(rowkey.getBytes());
			long timestamp=new Date().getTime();
		    for(int i=0;i<jr.size();i++) {
		    	JSONObject json=jr.getJSONObject(i);
		    	Set<String> sets=json.keySet();
		    	sets.forEach(s->{
		    	 	put.addColumn(Bytes.toBytes(json.getString("Family")==null?"default":json.getString("Family")),Bytes.toBytes(s),timestamp, Bytes.toBytes(json.getString(s)));
		    	});
		    }
		    table.put(put);
		    return Info.toJson("新增成功", "success");
		} catch (Exception e) {
			e.printStackTrace();
			return Info.toJson( e.getMessage(), "fail");
		} finally {
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	/**
	 * 同集群不同表数据转移
	 * @throws IOException 
	 */
	public  String InTableDatatransfer(String orgintable,String desttable,String zkip,String zkport) throws IOException {

		Connection conn = null;
		try {
		conn=initHbase(zkip,zkport);
		Table  otable=conn.getTable(TableName.valueOf(orgintable)); 
		Table dtable=conn.getTable(TableName.valueOf(desttable)); 
		 Scan scan = new Scan();
		 ResultScanner scanner = otable.getScanner(scan);
		 Map<String,Map<String,String>> result=new LinkedMap();
		 List<Put> puts=new LinkedList<Put>();
		 for (Result r :scanner){
			 r.listCells().forEach(cell -> {
					Put put=new Put(CellUtil.cloneRow(cell));
					put.addColumn(CellUtil.cloneFamily(cell),CellUtil.cloneQualifier(cell),cell.getTimestamp(),CellUtil.cloneValue(cell));
					puts.add(put);
			 }
			 );
		 }
		 dtable.put(puts); 
		 return "拷贝成功";
		}catch(Exception e) {
			 
			 return e.getMessage();
		 }finally {
			 conn.close();
		 }
	}
	
	private  Connection initHbase(String zkip,String zkport) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zkport);
        configuration.set("hbase.zookeeper.quorum", zkip);
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

}
