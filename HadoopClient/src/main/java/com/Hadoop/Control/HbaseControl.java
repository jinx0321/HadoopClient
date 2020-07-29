package com.Hadoop.Control;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import com.Hadoop.Utils.RequestUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.Hadoop.Log.*;

@Controller
public class HbaseControl {
	
	@Autowired
	com.Hadoop.Service.HbaseService HbaseService;
	
	@Autowired
	Info Info;
	
	@Autowired
	RequestUtils RequestUtils;
	
	@RequestMapping("/SearchHbase")
	@ResponseBody
	public String SearchHbase(HttpServletRequest request) {
		try {
		JSONObject json=RequestUtils.toJsonObject(request);
		return HbaseService.SearchTable(json.getString("TableName").trim(),json.getString("Rowkey").trim()==null?"":json.getString("Rowkey").trim(),json.getString("zkip").trim(),json.getString("zkport").trim());
		}catch(Exception e) {
			return e.getMessage();
		}
	}
	@RequestMapping("/HbaseClient")
	public String HbaseClient() {
		return "HbaseClient";
	}
	
	
	@RequestMapping("/Tabletransfer")
	@ResponseBody
	public String Tabletransfer(HttpServletRequest request) {
		try {
			JSONObject json=RequestUtils.toJsonObject(request);
			System.out.println(json);
			return HbaseService.InTableDatatransfer(json.getString("OrginTable").trim(),json.getString("DestTable").trim(),json.getString("zkip").trim(),json.getString("zkport").trim());
			}catch(Exception e) {
				return e.getMessage();
			}
		
	}
	
	@RequestMapping(value="/AddHbase",produces = "application/json;charset=UTF-8")
	@ResponseBody
	public String AddhHbase(HttpServletRequest request) {
		try {
		JSONObject json=RequestUtils.toJsonObject(request);
	    JSONArray jr=new JSONArray();
		String insertjson=json.getString("Insertdata");
		if(insertjson.startsWith("{")&&insertjson.endsWith("}")) {
			jr.add(JSONObject.parse(insertjson));
		}else {
			jr=JSONArray.parseArray(insertjson);
		}
		return HbaseService.InsertTable(json.getString("TableName").trim(),json.getString("Rowkey").trim(),json.getString("zkip").trim(),json.getString("zkport").trim(),jr);
		
		}catch(Exception e) {
			return Info.toJson(e.getMessage(), "fail");
		}
}

	
}
