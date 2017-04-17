package com.ns.testproject.sinanewsauto;

import java.io.FileOutputStream;

import org.apache.commons.io.IOUtils;

import com.ns.downloader.httpclient.HttpClientDao;

public class DownloadTest {
	
	public static void main(String[] args) {
		String url = "http://roll.news.sina.com.cn/news/gnxw/gdxw1/index.shtml";
		String proxy = null;
		String header = null;
		String postForm = null;
		try {
			String s = HttpClientDao.download(url, proxy, header, postForm, 2000, 20000);
			IOUtils.write(s, new FileOutputStream("/home/testproject/s.txt"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
