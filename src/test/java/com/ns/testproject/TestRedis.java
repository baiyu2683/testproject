package com.ns.testproject;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;

//import com.ns.storage.redis.JedisDao;

public class TestRedis {

	private static String key = "redis://:foobaredtrsadmin@192.9.200.18:6302/0";
	public static void main(String[] args) throws FileNotFoundException, IOException {
//		List<String> list = new ArrayList<>();
//		JedisDao.execute(key, jedis -> {
//			Set<String> s = jedis.keys("*");
//			Iterator<String> it = s.iterator();
//			while(it.hasNext()) {
//				String currentKey = it.next();
//				if(currentKey.startsWith("STRING_POINT") && currentKey.endsWith("-1")) {
//					list.add(currentKey);
//				}
//			}
//			return null;
//		});
////		IOUtils.writeLines(list, System.lineSeparator(), new FileOutputStream("f:/keys.txt"));
//		JedisDao.execute(key, jedis -> {
//			for(int i = 0; i < list.size(); i++)
//				jedis.del(list.get(i));
//			return null;
//		});
	}
}
