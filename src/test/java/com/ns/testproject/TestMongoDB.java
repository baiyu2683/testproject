package com.ns.testproject;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.ns.common.Initializer;
import com.ns.storage.mongodb.MongoDBDao;

public class TestMongoDB {
	
//	private static String CONNINFO_MONGODB = "mongodb://192.168.106.220:27017";
	private static String CONNINFO_MONGODB = "mongodb://localhost:27017";
	private static String DBNAME = "dc_crawler_test";
	private static String COLLECTION = "dc_data";
	
	@Test
	public void testCount() {
		System.out.println(MongoDBDao.count(CONNINFO_MONGODB, DBNAME, COLLECTION));
	}
	
	@Test
	public void createIndex() {
		MongoDBDao.createIndex(CONNINFO_MONGODB, DBNAME, COLLECTION, "IR_URLNAME");
	}
	
	@Test
	public void testQuery() throws FileNotFoundException, IOException {
		List<String> ex = new ArrayList<>();
		ex.add("IR_LASTTIME");
		ex.add("POINT");
		ex.add("CONFIG_NAME");
		ex.add("IR_SOURCEFILE");
		ex.add("IR_URLBODY");
		List<JSONObject> jsons = MongoDBDao.query(CONNINFO_MONGODB, DBNAME, COLLECTION, null, 1, 40000, ex.toArray(new String[5]));
		List<String> urls = jsons.parallelStream().map(url -> {
			return url.getString("IR_URLNAME");
		}).collect(Collectors.toList());
		IOUtils.writeLines(urls, System.lineSeparator(), new FileOutputStream("f:/urls.txt"));
	}
	
	@Test
	public void testUpsert() {
		String url = "78d0711c-14fc-44f9-8b17-52877d38efb6";
//		JSONObject where = new JSONObject();
//		where.put("IR_URLNAME", url);
		JSONObject newData = new JSONObject();
		newData.put("IR_URLNAME", url);
		newData.put("IR_URLBODY", "zhangheng3zhangheng");
		newData.put("POINT", UUID.randomUUID().toString() + "zhangheng1");
		newData.put("CONFIG_NAME", UUID.randomUUID().toString() + "zhangheng");
		newData.put("IR_LASTTIME", new Date());
		newData.put("IR_SOURCEFILE", "zhangheng12123");
	}
	
	@Test
	public void testStream() {
		List<JSONObject> dataCollection = new ArrayList<>();
		for(int i = 0; i < 10000; i++) {
			JSONObject newData = new JSONObject();
			newData.put("IR_URLNAME", UUID.randomUUID().toString());
			newData.put("IR_URLBODY", "zhangheng3");
			newData.put("POINT", UUID.randomUUID().toString() + "zhangheng1");
			newData.put("CONFIG_NAME", UUID.randomUUID().toString() + "zhangheng");
			newData.put("IR_LASTTIME", new Date());
			newData.put("IR_SOURCEFILE", "zhangheng12123");
			dataCollection.add(newData);
		}
		long start = System.currentTimeMillis();
		//120ms
//		List<Document> docs = dataCollection.parallelStream().map(jsonObject -> new Document(jsonObject)).collect(Collectors.toList());
		List<Document> docs = new ArrayList<>();
		//27ms
//		for(JSONObject json : dataCollection) {
//			docs.add(new Document(json));
//		}
		//35ms
//		int length = dataCollection.size();
//		for(int i = 0; i < length; i++) {
//			docs.add(new Document(dataCollection.get(i)));
//		}
		//25
		Iterator<JSONObject> it = dataCollection.iterator();
		while(it.hasNext()) {
			docs.add(new Document(it.next()));
		}
		System.out.println(System.currentTimeMillis() - start);
	}
	
	@Test
	public void test4() {
		ExecutorService es = Initializer.getSchedule(10, "a");
		for(int i = 0; i < 3; i++) {
			List<JSONObject> dataCollection = new ArrayList<>();
			for(int j = 0; j < 5000; j++) {
				JSONObject newData = new JSONObject();
				newData.put("IR_URLNAME", UUID.randomUUID().toString());
				newData.put("IR_URLBODY", "zhangheng3");
				newData.put("POINT", UUID.randomUUID().toString() + "zhangheng1");
				newData.put("CONFIG_NAME", UUID.randomUUID().toString() + "zhangheng");
				newData.put("IR_LASTTIME", new Date());
				newData.put("POINTKEY", UUID.randomUUID().toString() + "zhangheng");
				dataCollection.add(newData);
			}
			es.submit(new Task(dataCollection));
		}
		try {
			TimeUnit.SECONDS.sleep(1000000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static class Task implements Runnable {
		
		private List<JSONObject> dataCollection;
		
		Task(List<JSONObject> dataCollection) {
			this.dataCollection = dataCollection;
		}

		@Override
		public void run() {
			MongoDBDao.delayImportRecords(CONNINFO_MONGODB, DBNAME, COLLECTION, dataCollection);
		}
		
	}
}
