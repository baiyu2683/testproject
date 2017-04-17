package com.ns.testproject.mongodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.UpdateResult;
import com.ns.common.Initializer;
import com.ns.storage.mongodb.MongoDBDao;
import com.ns.utils.DeflateUtil;

/**
 * upsert
 * @author zhangheng
 *
 */
public class Upsert implements Runnable {

	private static String CONNINFO_MONGODB = "mongodb://192.168.66.3:50009";
//	private static String CONNINFO_MONGODB = "mongodb://192.168.106.220:27017";
//	private static String CONNINFO_MONGODB = "mongodb://localhost:27017";
	private static String DBNAME = "dc_crawler_test";
	private static String COLLECTION = "dc_data";
	private static long threadNum;
	private static long dataNum;
	private static long batchSize;
	private static String conf;
	private static String url;
	
	private static String htmlSourceFile;
	private static String compressionSource;
	private static String source;
	
	//数据队列
	private static LinkedBlockingQueue<JSONObject> datas = new LinkedBlockingQueue<>();
	//时间累计
	private static AtomicLong timeAll = new AtomicLong();
	private static CountDownLatch latch;
	
	static {
		conf = Initializer.CONF + "/conf.properties";
		url = Initializer.CONF + "/urls.txt";
		Properties p = new Properties();
		try {
			p.load(new FileInputStream(conf));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		threadNum = Long.parseLong(p.getProperty("threadNum"));
		dataNum = Long.parseLong(p.getProperty("dataNum"));
		batchSize = Long.parseLong(p.getProperty("batchSize"));
		latch = new CountDownLatch((int)threadNum);
		
		htmlSourceFile = Initializer.CONF + "/test.html";
		File html = new File(htmlSourceFile);
		try {
			source = IOUtils.toString(new FileInputStream(html), "utf-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
		compressionSource = DeflateUtil.compression(source);
	}

	@Override
	public void run() {
		System.out.println("更新任务开始...mongodb:" + CONNINFO_MONGODB);
		List<String> urlList = new ArrayList<>();
		try {
			urlList = IOUtils.readLines(new FileInputStream(url));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Random r = new Random(47L);
	    List<String> tempUrls = new ArrayList<>();
	    for (int i = 0; i < dataNum; ++i) {
	      if (i % 2 != 0)
	        tempUrls.add((String)urlList.get(r.nextInt(urlList.size())));
	      else {
	        tempUrls.add(UUID.randomUUID().toString());
	      }
	    }
	    urlList.clear();
	    String compressionSource = DeflateUtil.compression(source);
	    List<JSONObject> list = new ArrayList<>();
	    Random rand = new Random(47);
	    for (int i = 0; i < dataNum; ++i) {
	      JSONObject obj = new JSONObject();
	      obj.put("IR_URLNAME", tempUrls.get(i) + rand.nextDouble());
	      obj.put("IR_URLBODY", compressionSource);
	      obj.put("POINT", UUID.randomUUID().toString() + rand.nextDouble());
	      obj.put("CONFIG_NAME", UUID.randomUUID().toString() + rand.nextDouble());
	      obj.put("IR_LASTTIME", new Date());
	      list.add(obj);
	    }
	    tempUrls.clear();
	    datas.addAll(list);
		System.out.println("数据量:" + datas.size());
		//启动任务线程
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.submit(new StopTask());
		executor.submit(new LogTask());
		
		for(int i = 0; i < threadNum; i++) {
			executor.submit(new UpsertTask());
		}
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static class StopTask implements Runnable {
		@Override
		public void run() {
			long start = System.currentTimeMillis();
			while(!Thread.interrupted()) {
				if(datas.size() == 0) {
					try {
						latch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println("总时间:" + timeAll.get());
					System.out.println("运行时间:" + (System.currentTimeMillis() - start));
					System.exit(0);
				}
			}
		}
	}
	
	private static class LogTask implements Runnable {
		@Override
		public void run() {
			while(!Thread.interrupted()) {
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("余量:" + datas.size());
			}
		}
	}
	
	static class UpsertTask implements Runnable {
		@Override
		public void run() {
			while(!Thread.interrupted()) {
				boolean isSuccess = false;
				long time = 0;
				List<JSONObject> list = new ArrayList<>();
				try {
					while(list.size() < batchSize && datas.size() != 0) {
						JSONObject obj = datas.poll();
						if(obj != null) {
							list.add(obj);
						}
					}
					//
					long start = System.currentTimeMillis();
					BulkWriteResult bwr = MongoDBDao.upsert(CONNINFO_MONGODB, DBNAME, COLLECTION, list);
					time = System.currentTimeMillis() - start;
					System.out.println(time + ", update:" + bwr.getModifiedCount() + ", upsert:" + bwr.getUpserts().size());
					isSuccess = true;
				} catch(Exception e) {
					e.printStackTrace();
				}finally {
					System.out.println("结束。。");
					if(isSuccess) timeAll.addAndGet(time);
					list.clear();
					if(datas.size() == 0) {
						latch.countDown();
						break;
					}
				}
			}
			System.out.println(Thread.currentThread().getName() + " exit...");
		}
	}
}
