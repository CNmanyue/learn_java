package com.my.redis;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * <h3>概要:</h3> redis客户端池管理类 <br>
 * <h3>履历:</h3>
 * <ol>
 * <li>2016年4月6日[zhouxw] 新建</li>
 * </ol>
 */
public class RedisDataSource {

	private static final Logger log = Logger.getLogger(RedisDataSource.class);
	private static RedisDataSource redisDataSource = null;

	/** 切片池 */
	private ShardedJedisPool shardedJedisPool;
	/** 非切片池 */
	private JedisPool jedisPool;
	/** 池配置 */
	private JedisPoolConfig jedisPoolConfig;

	private RedisDataSource() {
		if (jedisPoolConfig == null) {
			setJedisPoolConfig();
		}
		if (jedisPool == null) {
			setJedisPool();
		}
		if (shardedJedisPool == null) {
			setShardedJedisPool();
		}
	}

	public synchronized static RedisDataSource getInstance() {
		if (redisDataSource == null) {
			redisDataSource = new RedisDataSource();
		}
		return redisDataSource;
	}

	/**
	 * <b>概要：</b>取得ShardedJedis的客户端，可以执行命令了。 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月31日 </br>
	 * @return
	 */
	public ShardedJedis getShardedJedisClient() {
		try {
			ShardedJedis shardJedis = shardedJedisPool.getResource();
			return shardJedis;
		} catch (Exception e) {
			log.error("getRedisClient error", e);
		}
		return null;
	}

	/**
	 * <b>概要：</b>将资源返还给pool <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月31日 </br>
	 * @param shardedJedis
	 */
	public void returnShardedJedisResource(ShardedJedis shardedJedis) {
		shardedJedisPool.returnResource(shardedJedis);
	}

	/**
	 * <b>概要：</b>出现异常后，将资源返还给pool （其实不需要第二个方法） <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月31日 </br>
	 * @param shardedJedis
	 * @param broken
	 */
	public void returnShardedJedisResource(ShardedJedis shardedJedis, boolean broken) {
		if (broken) {
			shardedJedisPool.returnBrokenResource(shardedJedis);
		} else {
			shardedJedisPool.returnResource(shardedJedis);
		}
	}

	/**
	 * <b>概要：</b>取得redis的客户端，可以执行命令了。 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月31日 </br>
	 * @return
	 */
	public Jedis getJedisClient() {
		try {
			Jedis jedis = jedisPool.getResource();
			return jedis;
		} catch (Exception e) {
			log.error("getJedisClient error", e);
		}
		return null;
	}

	/**
	 * <b>概要：</b>将资源返还给pool <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月31日 </br>
	 * @param Jedis
	 */
	public void returnJedisResource(Jedis jedis) {
		jedisPool.returnResource(jedis);
	}

	/**
	 * <b>概要：</b>出现异常后，将资源返还给pool （其实不需要第二个方法） <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月31日 </br>
	 * @param Jedis
	 * @param broken
	 */
	public void returnJedisResource(Jedis jedis, boolean broken) {
		if (broken) {
			jedisPool.returnBrokenResource(jedis);
		} else {
			jedisPool.returnResource(jedis);
		}
	}

	/**
	 * <b>概要：</b>创建切片连接池 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年4月6日 </br>
	 */
	private void setShardedJedisPool() {
		List<JedisShardInfo> jdsInfoList = new ArrayList<JedisShardInfo>(2);

		JedisShardInfo infoA = new JedisShardInfo(RedisConfig.HOST, RedisConfig.PORT);
		infoA.setPassword(RedisConfig.PASSWD);
		jdsInfoList.add(infoA);

		shardedJedisPool = new ShardedJedisPool(this.jedisPoolConfig, jdsInfoList);
		log.info("###setShardedJedisPool 创建切片连接池");
	}

	/**
	 * <b>概要：</b>创建非切片连接池 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年4月6日 </br>
	 */
	private void setJedisPool() {
		// 无需设置密码的：本地运行用
		jedisPool = new JedisPool(jedisPoolConfig, RedisConfig.HOST, RedisConfig.PORT);
		/*
		 * 连接时加密码的：测试及生产环境用 timeout:当连接空闲时多长时间后，关闭此连接，0则不关闭 database:默认数据库
		 */
		jedisPool = new JedisPool(jedisPoolConfig, RedisConfig.HOST, RedisConfig.PORT, 0, RedisConfig.PASSWD, RedisConfig.DATABASE);
		log.info("###setJedisPool 创建非切片连接池");
	}

	private void setJedisPoolConfig() {
		this.jedisPoolConfig = new JedisPoolConfig();
		this.jedisPoolConfig.setMaxTotal(RedisConfig.MAX_ACTIVE);// 最大活动的对象个数
		this.jedisPoolConfig.setMaxIdle(RedisConfig.MAX_IDLE);// 对象最大空闲时间
		this.jedisPoolConfig.setMaxWaitMillis(RedisConfig.MAX_WAIT);// 获取对象时最大等待时间
		this.jedisPoolConfig.setTestOnBorrow(RedisConfig.TEST_ON_BORROW);
		log.info("###setJedisPoolConfig 创建jedis连接池配置");
	}

}
