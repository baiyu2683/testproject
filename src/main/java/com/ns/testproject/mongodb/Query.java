package com.ns.testproject.mongodb;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
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
import com.ns.common.Initializer;
import com.ns.storage.mongodb.MongoDBDao;

public class Query implements Runnable {
	
	private static String CONNINFO_MONGODB = "mongodb://192.168.66.3:50009";
//	private static String CONNINFO_MONGODB = "mongodb://localhost:27017";
	private static String DBNAME = "dc_crawler_test";
	private static String COLLECTION = "dc_data";
	private static long threadNum;
	private static long dataNum;
	private static String conf;
	private static String url;
	//数据队列
	private static LinkedBlockingQueue<String> datas = new LinkedBlockingQueue<>();
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
		latch = new CountDownLatch((int)threadNum);
	}

	public void run() {
		System.out.println("查询任务开始...");
		List<String> urlList = new ArrayList<>();
		try {
			urlList = IOUtils.readLines(new FileInputStream(url));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("dataNum:" + dataNum);
		datas.addAll(urlList);
		for(int i = 0; i < (dataNum - urlList.size()); i++) {
			datas.add(UUID.randomUUID().toString());
		}
		System.out.println(datas.size());
		//启动任务线程
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.submit(new StopTask());
		executor.submit(new LogTask());
		
		for(int i = 0; i < threadNum; i++) {
			executor.submit(new QueryTask());
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
	
	private static AtomicLong counter1 = new AtomicLong(0);
	static class QueryTask implements Runnable {
		@Override
		public void run() {
			while(!Thread.interrupted()) {
				boolean isSuccess = false;
				long time = 0;
				try {
					String url = null;
					url = datas.poll();
					if(url == null) continue;
					JSONObject obj = new JSONObject();
					obj.put("IR_URLNAME", url);
					long start = System.currentTimeMillis();
					List<JSONObject> listResult = MongoDBDao.query(CONNINFO_MONGODB, DBNAME, COLLECTION, obj, 1, 1);
					time = System.currentTimeMillis() - start;
					if(listResult.size() > 0) {
						JSONObject obj1 = listResult.iterator().next();
						System.out.println(obj1.getString("IR_URLNAME"));
						if(counter1.getAndIncrement() <= 0) {
							System.out.println(obj1.get("IR_URLBODY"));	
						}
					}
					listResult.clear();
					System.out.println(time);
					isSuccess = true;
				} catch(Exception e) {
				}finally {
					if(isSuccess) timeAll.addAndGet(time);
					if(datas.size() == 0) {
						latch.countDown();
						break;
					}
				}
			}
		}
	}

}
