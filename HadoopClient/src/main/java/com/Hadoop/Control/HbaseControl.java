package com.Hadoop.Control;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class HbaseControl {
	
	@Autowired
	com.Hadoop.Service.HbaseService HbaseService;
	@RequestMapping("/hbasesearch")
	@ResponseBody
	public String hbasesearch(HttpServletRequest request) {
		return HbaseService.SearchTable(request.getParameter("TabelName").trim(), request.getParameter("Rowkey").trim(), request.getParameter("zkip").trim(), request.getParameter("zkport").trim());
	}
	@RequestMapping("/hbaseclient")
	public String hbaseclient(HttpServletRequest request) {
		return "HbaseClient";
	}
}
