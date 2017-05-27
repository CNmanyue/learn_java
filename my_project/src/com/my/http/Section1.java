package com.my.http;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class Section1 {

	/**
	 * 发一个请求
	 * @author: zhouxw
	 * @date: 2017年5月16日
	 * @param args
	 */
	public static void main(String[] args) {
		// 1. 创建一个默认客户端
		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			// 2. 声名一个HttpRequest
			HttpGet get = new HttpGet("http://10.162.84.252:8081/api/");
			// 3. 执行请求，接收Response
			response = client.execute(get);
			// 4. 消费实体:依据实体的内容来源，HttpClient区分出三种实体
			// a.流式实体（streamed）：内容来源于一个流，或者在运行中产生【译者：原文为generated on the fly】。这个类别包括从响应中接收到的实体。流式实体不可重复。
			// b.自包含实体（self-contained）：在内存中的内容或者通过独立的连接/其他实体获得的内容。自包含实体是可重复的。这类实体大部分是HTTP内含实体请求。
			// c.包装实体（wrapping）：从另外一个实体中获取内容。
			Header[] headers = response.getHeaders("Set-Cookie");
			System.out.println("header length:" + headers.length);
			for (int i = 0; i < headers.length; i++) {
				System.out.println(headers[i].getName() + ":" + headers[i].getValue());
			}
			HttpEntity entity = response.getEntity();
			InputStream in = entity.getContent(); // 第一种消费实体的方法
			Header contentType = entity.getContentType();
			System.out.println(contentType);
			byte[] readBuffer = new byte[in.available()];
			System.out.println("读取字节:" + in.read(readBuffer));
			System.out.println("========start=========");
			System.out.println(new String(readBuffer));
			System.out.println("========end=========");
			in.close(); // 关闭实体流
			// OutputStream out = new FileOutputStream("D:\\00000320\\20170516.txt");
			// entity.writeTo(out); // 第二种消费实体的方法
			// out.flush();
			// out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				response.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
