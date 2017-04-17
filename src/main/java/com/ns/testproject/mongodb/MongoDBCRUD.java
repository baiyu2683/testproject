package com.ns.testproject.mongodb;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.result.UpdateResult;
import com.ns.common.Initializer;
import com.ns.storage.mongodb.MongoDBDao;

/**
 * 测试mongodb操作
 * @author zhangheng
 *
 */
public class MongoDBCRUD {
	
	//操作:查询，插入
	static enum Operator {
		query, insert, upsert
	}
	
    public static void main( String[] args ) {
    	String conf = Initializer.CONF + "/conf.properties";
    	Properties p = new Properties();
		try {
			p.load(new FileInputStream(conf));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//删除测试数据库及以下所有集合
		String CONNINFO_MONGODB = "mongodb://192.168.66.3:50009";
//		MongoDBDao.dropDatabase(CONNINFO_MONGODB, "dc_crawler_test");

		//建立在库dc_crawler_test的dc_data集合中以IR_URLNAME升序建立索引
//		MongoDBDao.createIndex(CONNINFO_MONGODB, "dc_crawler_test", "dc_data", "IR_URLNAME");
		
		//查询个数
//		List<String> s = MongoDBDao.findAllDbname(CONNINFO_MONGODB);
//		System.out.println(JSON.toJSON(s));
//		System.out.println("集合大小:" + MongoDBDao.count(CONNINFO_MONGODB, "dc_crawler_test", "dc_data"));
		
//		Random r = new Random(47);
//		String url = "380b6f7b-cb97-4e51-aa7e-72d43585cd18";
//		JSONObject where = new JSONObject();
//		where.put("IR_URLNAME", url);
//		JSONObject newData = new JSONObject();
//		newData.put("IR_URLNAME", url);
//		newData.put("IR_URLBODY", r.nextDouble() + "");
//		newData.put("POINT", UUID.randomUUID().toString() + r.nextDouble());
//		newData.put("CONFIG_NAME", UUID.randomUUID().toString() + r.nextDouble());
//		newData.put("IR_LASTTIME", new Date());
//		newData.put("IR_SOURCEFILE", r.nextDouble() + "");
//		System.out.println("where:" + where);
//		System.out.println("newData:" + newData);
//		UpdateResult ur = MongoDBDao.upsertOne(CONNINFO_MONGODB, "dc_crawler_test", "dc_data", where, newData);
//		System.out.println(JSON.toJSON(ur));
		//测试查询和插入
		String queryOrInsert = p.getProperty("operator");
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Operator o = Operator.query;
        try{
        	o = Operator.valueOf(queryOrInsert);
        } catch(Exception e){
        	e.printStackTrace();
        }
        switch(o) {
    		case query:{
    			System.out.println("query");
    			executor.submit(new Query());break;
    		}
    		case insert:{
    			System.out.println("insert");
    			executor.submit(new Insert());break;
    		}
    		case upsert:{
    			System.out.println("upsert");
    			executor.submit(new Upsert());break;
    		}
        }
        try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
