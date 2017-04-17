package com.ns.testproject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.Test;

public class TestDate {

	
	@Test
	public void test1() throws ParseException {
		String dateString = "2017-04-12T07:14:27.79Z";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(format.format(sdf.parse(dateString)));
	}
}
