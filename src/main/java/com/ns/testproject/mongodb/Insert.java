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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import com.ns.common.Initializer;
import com.ns.storage.mongodb.MongoDBDao;
import com.ns.utils.DeflateUtil;

public class Insert implements Runnable {

	private static String CONNINFO_MONGODB = "mongodb://192.168.66.3:50009";
//	private static String CONNINFO_MONGODB = "mongodb://localhost:27017";
	private static String DBNAME = "dc_crawler_test";
	private static String COLLECTION = "dc_data";
	private static long threadNum;
	private static long dataNum;
	private static long batchSize;
	private static String htmlSourceFile;
	private static String conf;
	private static String compressionSource;
	private static String source;
	//数据队列
	private static LinkedBlockingQueue<JSONObject> datas = new LinkedBlockingQueue<>();
	//时间累计
	private static AtomicLong timeAll = new AtomicLong();
	private static CountDownLatch latch;
	
	static {
		htmlSourceFile = Initializer.CONF + "/test.html";
		conf = Initializer.CONF + "/conf.properties";
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
		
		File html = new File(htmlSourceFile);
		try {
			source = IOUtils.toString(new FileInputStream(html), "utf-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
		compressionSource = DeflateUtil.compression(source);
	}
	
	public void run() {
		System.out.println("插入任务开始...");
		
		for(int i = 0; i < dataNum; i++) {
			JSONObject obj = new JSONObject();
			obj.put("IR_URLNAME", UUID.randomUUID().toString());
			obj.put("IR_URLBODY", compressionSource);
			obj.put("POINT", UUID.randomUUID().toString());
			obj.put("CONFIG_NAME", UUID.randomUUID().toString());
			obj.put("IR_LASTTIME", new Date());
//			obj.put("IR_SOURCEFILE", source);
			datas.add(obj);
		}
		System.out.println("总条数:" + datas.size());
		//启动任务线程
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.submit(new StopTask());
		executor.submit(new LogTask());
//		executor.submit(new Producer());
		
		for(int i = 0; i < threadNum; i++) {
			executor.submit(new InsertTask());
		}
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler(){
			@Override
			public void uncaughtException(Thread arg0, Throwable arg1) {
				System.out.println(arg0.getId() + ":" + arg1.getMessage());
			}
		});
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
					System.out.println("等待数据入库完成...");
					try {
						latch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println("集合大小:" + MongoDBDao.count(CONNINFO_MONGODB, DBNAME, COLLECTION));
					System.out.println("插入成功数:" + successInsertCounter.get());
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
	
	private static AtomicLong threadCounter = new AtomicLong(0);
	private static AtomicLong successInsertCounter = new AtomicLong(0);
	
	static class InsertTask implements Runnable {

		@Override
		public void run() {
			while(!Thread.interrupted()) {
				boolean isSuccess = false;
				long time = 0;
				List<JSONObject> list = new ArrayList<>();
				try {
					while(list.size() < batchSize) {
						JSONObject obj = datas.poll();
						if(obj != null) {
							list.add(obj);
						} else {
							break;
						}
					}
					if(list.size() > 0) {
						long start = System.currentTimeMillis();
						MongoDBDao.insert(CONNINFO_MONGODB, DBNAME, COLLECTION, list);
						time = System.currentTimeMillis() - start;
						System.out.println("计数:" + threadCounter.incrementAndGet() + ",个数:" + list.size() + "-" +time);
						successInsertCounter.addAndGet(list.size());
						isSuccess = true;
					}
				} finally {
					if(isSuccess) timeAll.addAndGet(time);
					if(list.size() > 0) list.clear();
					if(datas.size() == 0) {
						latch.countDown();
						break;
					}
				}
			}
		}
	}
}
