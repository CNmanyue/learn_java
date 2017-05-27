package com.my.redis;

public interface RedisConfig {

	public static String HOST = "localhost";
	public static int PORT = 6379;
	public static String PASSWD = "zhouxw";
	public static int DATABASE = 0;	// 0-15
	public static int MAX_ACTIVE = 500;
	public static int MAX_IDLE = 60000;
	public static int MAX_WAIT = 10000;
	public static boolean TEST_ON_BORROW = true;
	public static boolean TEST_ON_RETURN = true;
}
