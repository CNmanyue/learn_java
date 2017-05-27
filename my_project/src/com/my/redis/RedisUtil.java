package com.my.redis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

/**
 * <h3>概要:</h3> RedisClient <br>
 * <h3>履历:</h3>
 * <ol>
 * <li>2016年3月31日[zhouxw] 新建</li>
 * </ol>
 */
public class RedisUtil {

	private static final Logger log = Logger.getLogger(RedisUtil.class);

	private static RedisDataSource redisDataSource = RedisDataSource.getInstance();

	public RedisUtil() {
	}

	public static void disconnect(Jedis jedis) {
		jedis.disconnect();
	}

	public static Jedis newJedisConn() {
		return redisDataSource.getJedisClient();
	}

	/**
	 * 设置单个值
	 * @param key
	 * @param value
	 * @return
	 */
	public static String set(String key, String value) {
		String result = null;

		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.set(key, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	/**
	 * 获取单个值
	 * @param key
	 * @return
	 */
	public static String get(String key) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}

		boolean broken = false;
		try {
			result = jedis.get(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Boolean exists(String key) {
		Boolean result = false;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.exists(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String type(String key) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.type(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	/**
	 * 在某段时间后实现
	 * @param key
	 * @param unixTime
	 * @return
	 */
	public static Long expire(String key, int seconds) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.expire(key, seconds);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	/**
	 * 在某个时间点失效
	 * @param key
	 * @param unixTime
	 * @return
	 */
	public static Long expireAt(String key, long unixTime) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.expireAt(key, unixTime);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long ttl(String key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.ttl(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static boolean setbit(String key, long offset, boolean value) {

		Jedis jedis = redisDataSource.getJedisClient();
		boolean result = false;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.setbit(key, offset, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static boolean getbit(String key, long offset) {
		Jedis jedis = redisDataSource.getJedisClient();
		boolean result = false;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;

		try {
			result = jedis.getbit(key, offset);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static long setrange(String key, long offset, String value) {
		Jedis jedis = redisDataSource.getJedisClient();
		long result = 0;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.setrange(key, offset, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String getrange(String key, long startOffset, long endOffset) {
		Jedis jedis = redisDataSource.getJedisClient();
		String result = null;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.getrange(key, startOffset, endOffset);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String getSet(String key, String value) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.getSet(key, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long setnx(String key, String value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.setnx(key, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String setex(String key, int seconds, String value) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.setex(key, seconds, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long decrBy(String key, long integer) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.decrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long decr(String key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.decr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long incrBy(String key, long integer) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.incrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long incr(String key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.incr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long append(String key, String value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.append(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String substr(String key, int start, int end) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.substr(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hset(String key, String field, String value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hset(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String hget(String key, String field) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hget(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hsetnx(String key, String field, String value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hsetnx(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String hmset(String key, Map<String, String> hash) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hmset(key, hash);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<String> hmget(String key, String... fields) {
		List<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hmget(key, fields);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hincrBy(String key, String field, long value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hincrBy(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Boolean hexists(String key, String field) {
		Boolean result = false;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hexists(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long del(String key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.del(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hdel(String key, String field) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hdel(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hlen(String key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hlen(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> hkeys(String key) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hkeys(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<String> hvals(String key) {
		List<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hvals(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Map<String, String> hgetAll(String key) {
		Map<String, String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.hgetAll(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	// ================list ====== l表示 list或 left, r表示right====================
	public static Long rpush(String key, String string) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.rpush(key, string);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long lpush(String key, String string) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.lpush(key, string);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long llen(String key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.llen(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<String> lrange(String key, long start, long end) {
		List<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.lrange(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String ltrim(String key, long start, long end) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.ltrim(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String lindex(String key, long index) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.lindex(key, index);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String lset(String key, long index, String value) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.lset(key, index, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long lrem(String key, long count, String value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.lrem(key, count, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String lpop(String key) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.lpop(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String rpop(String key) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.rpop(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	// return 1 add a not exist value ,
	// return 0 add a exist value
	public static Long sadd(String key, String member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.sadd(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> smembers(String key) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.smembers(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long srem(String key, String member) {
		Jedis jedis = redisDataSource.getJedisClient();

		Long result = null;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.srem(key, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String spop(String key) {
		Jedis jedis = redisDataSource.getJedisClient();
		String result = null;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.spop(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long scard(String key) {
		Jedis jedis = redisDataSource.getJedisClient();
		Long result = null;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.scard(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Boolean sismember(String key, String member) {
		Jedis jedis = redisDataSource.getJedisClient();
		Boolean result = null;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.sismember(key, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String srandmember(String key) {
		Jedis jedis = redisDataSource.getJedisClient();
		String result = null;
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.srandmember(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zadd(String key, double score, String member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.zadd(key, score, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> zrange(String key, int start, int end) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.zrange(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zrem(String key, String member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = jedis.zrem(key, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Double zincrby(String key, double score, String member) {
		Double result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zincrby(key, score, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zrank(String key, String member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrank(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zrevrank(String key, String member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrank(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> zrevrange(String key, int start, int end) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrange(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeWithScores(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeWithScores(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zcard(String key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zcard(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Double zscore(String key, String member) {
		Double result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zscore(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<String> sort(String key) {
		List<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.sort(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<String> sort(String key, SortingParams sortingParameters) {
		List<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.sort(key, sortingParameters);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zcount(String key, double min, double max) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zcount(key, min, max);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> zrangeByScore(String key, double min, double max) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScore(key, min, max);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> zrevrangeByScore(String key, double max, double min) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScore(key, max, min);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScore(key, min, max, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		Set<String> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScore(key, max, min, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScoreWithScores(key, min, max);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScoreWithScores(key, max, min);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScoreWithScores(key, min, max, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zremrangeByRank(String key, int start, int end) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zremrangeByRank(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zremrangeByScore(String key, double start, double end) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zremrangeByScore(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.linsert(key, where, pivot, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String set(byte[] key, byte[] value) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.set(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] get(byte[] key) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.get(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Boolean exists(byte[] key) {
		Boolean result = false;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.exists(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String type(byte[] key) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.type(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long expire(byte[] key, int seconds) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.expire(key, seconds);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long expireAt(byte[] key, long unixTime) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.expireAt(key, unixTime);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long ttl(byte[] key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.ttl(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] getSet(byte[] key, byte[] value) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.getSet(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long setnx(byte[] key, byte[] value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.setnx(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String setex(byte[] key, int seconds, byte[] value) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.setex(key, seconds, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long decrBy(byte[] key, long integer) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.decrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long decr(byte[] key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.decr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long incrBy(byte[] key, long integer) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.incrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long incr(byte[] key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.incr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long append(byte[] key, byte[] value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.append(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] substr(byte[] key, int start, int end) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.substr(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hset(byte[] key, byte[] field, byte[] value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hset(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] hget(byte[] key, byte[] field) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hget(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hsetnx(byte[] key, byte[] field, byte[] value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hsetnx(key, field, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String hmset(byte[] key, Map<byte[], byte[]> hash) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hmset(key, hash);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<byte[]> hmget(byte[] key, byte[]... fields) {
		List<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hmget(key, fields);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hincrBy(byte[] key, byte[] field, long value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hincrBy(key, field, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Boolean hexists(byte[] key, byte[] field) {
		Boolean result = false;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hexists(key, field);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hdel(byte[] key, byte[] field) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hdel(key, field);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long hlen(byte[] key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hlen(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> hkeys(byte[] key) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hkeys(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Collection<byte[]> hvals(byte[] key) {
		Collection<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hvals(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Map<byte[], byte[]> hgetAll(byte[] key) {
		Map<byte[], byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.hgetAll(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long rpush(byte[] key, byte[] string) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.rpush(key, string);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long lpush(byte[] key, byte[] string) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.lpush(key, string);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long llen(byte[] key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.llen(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<byte[]> lrange(byte[] key, int start, int end) {
		List<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.lrange(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String ltrim(byte[] key, int start, int end) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.ltrim(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] lindex(byte[] key, int index) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.lindex(key, index);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static String lset(byte[] key, int index, byte[] value) {
		String result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.lset(key, index, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long lrem(byte[] key, int count, byte[] value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.lrem(key, count, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] lpop(byte[] key) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.lpop(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] rpop(byte[] key) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.rpop(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long sadd(byte[] key, byte[] member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.sadd(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> smembers(byte[] key) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.smembers(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long srem(byte[] key, byte[] member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.srem(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] spop(byte[] key) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.spop(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long scard(byte[] key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.scard(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Boolean sismember(byte[] key, byte[] member) {
		Boolean result = false;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.sismember(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static byte[] srandmember(byte[] key) {
		byte[] result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.srandmember(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zadd(byte[] key, double score, byte[] member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zadd(key, score, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> zrange(byte[] key, int start, int end) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrange(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zrem(byte[] key, byte[] member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrem(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Double zincrby(byte[] key, double score, byte[] member) {
		Double result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zincrby(key, score, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zrank(byte[] key, byte[] member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrank(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zrevrank(byte[] key, byte[] member) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrank(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> zrevrange(byte[] key, int start, int end) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrange(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeWithScores(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeWithScores(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zcard(byte[] key) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zcard(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Double zscore(byte[] key, byte[] member) {
		Double result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zscore(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<byte[]> sort(byte[] key) {
		List<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.sort(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
		List<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.sort(key, sortingParameters);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zcount(byte[] key, double min, double max) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zcount(key, min, max);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScore(key, min, max);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScore(key, min, max, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScoreWithScores(key, min, max);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrangeByScoreWithScores(key, min, max, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScore(key, max, min);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
		Set<byte[]> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScore(key, max, min, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScoreWithScores(key, max, min);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zremrangeByRank(byte[] key, int start, int end) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zremrangeByRank(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long zremrangeByScore(byte[] key, double start, double end) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.zremrangeByScore(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	public static Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
		Long result = null;
		Jedis jedis = redisDataSource.getJedisClient();
		if (jedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = jedis.linsert(key, where, pivot, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnJedisResource(jedis, broken);
		}
		return result;
	}

	@SuppressWarnings("deprecation")
	public static List<Object> pipelined(ShardedJedisPipeline jedisPipeline) {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		List<Object> result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.pipelined(jedisPipeline);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	public static Jedis getShard(byte[] key) {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		Jedis result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShard(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	public static Jedis getShard(String key) {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		Jedis result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShard(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	public static JedisShardInfo getShardInfo(byte[] key) {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		JedisShardInfo result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShardInfo(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	public static JedisShardInfo getShardInfo(String key) {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		JedisShardInfo result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShardInfo(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	public static String getKeyTag(String key) {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		String result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getKeyTag(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	public static Collection<JedisShardInfo> getAllShardInfo() {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		Collection<JedisShardInfo> result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getAllShardInfo();

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	public static Collection<Jedis> getAllShards() {
		ShardedJedis shardedJedis = redisDataSource.getShardedJedisClient();
		Collection<Jedis> result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getAllShards();

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			redisDataSource.returnShardedJedisResource(shardedJedis, broken);
		}
		return result;
	}

	// ==========================锁操作 start================================
	/**
	 * <b>概要：</b>获取一个锁，尝试10秒 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月30日 </br>
	 * @param client RedisClientTemplate对象
	 * @param lockName 锁名称
	 * @return
	 */
	public static String acquireLock(String lockName) {
		return acquireLock(lockName, 10000);
	}

	/**
	 * <b>概要：</b>获取一个锁 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月30日 </br>
	 * @param client RedisClientTemplate对象
	 * @param lockName 锁名称
	 * @param acquireTimeout 尝试获取锁的时间，ms
	 * @return
	 */
	public static String acquireLock(String lockName, long acquireTimeout) {
		String identifier = UUID.randomUUID().toString();

		long end = System.currentTimeMillis() + acquireTimeout;
		while (System.currentTimeMillis() < end) {
			if (setnx("lock:" + lockName, identifier) == 1) {
				return identifier;
			}

			try {
				Thread.sleep(1);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		}

		return null;
	}

	/**
	 * <b>概要：</b>获取一个锁（可配置超时时间） <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月30日 </br>
	 * @param client RedisClientTemplate对象
	 * @param lockName 锁名称
	 * @param acquireTimeout 尝试获取锁的时间，ms
	 * @param lockTimeout 超时时间，ms
	 * @return 返回null则获取失败
	 */
	public static String acquireLockWithTimeout(String lockName, long acquireTimeout, long lockTimeout) {
		String identifier = UUID.randomUUID().toString();
		String lockKey = "lock:" + lockName;
		int lockExpire = (int) (lockTimeout / 1000);

		long end = System.currentTimeMillis() + acquireTimeout;
		while (System.currentTimeMillis() < end) {
			if (setnx(lockKey, identifier) == 1) {
				expire(lockKey, lockExpire);
				return identifier;
			}
			if (ttl(lockKey) == -1) {
				expire(lockKey, lockExpire);
			}

			try {
				Thread.sleep(1);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		}

		// null indicates that the lock was not acquired
		return null;
	}

	/**
	 * <b>概要：</b>释放锁 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年3月30日 </br>
	 * @param client RedisClientTemplate对象
	 * @param lockName 锁名称
	 * @param identifier 锁值（唯一）
	 * @return
	 */
	public static boolean releaseLock(String lockName, String identifier) {
		String lockKey = "lock:" + lockName;
		while (true) {
			if (identifier.equals(get(lockKey))) {
				if (del(lockKey) != 1) {
					continue;
				}
				return true;
			}

			break;
		}

		return false;
	}

	// ==========================锁操作 end================================

	/**
	 * <b>概要：</b>返回有序集合第一个准备就绪的成员 <b>作者：</b>zhouxw </br> <b>日期：</b>2016年4月6日 </br>
	 * @param queueName 有序集合名称
	 * @return 没有则等待10ms并返回null
	 */
	public static Tuple getTimeHasComeItem(String queueName) {
		Set<Tuple> items = RedisUtil.zrangeWithScores(queueName, 0, 0);
		Tuple item = items.size() > 0 ? items.iterator().next() : null;
		if (item == null || item.getScore() > System.currentTimeMillis()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException ie) {
				Thread.interrupted();
			}
			return null;
		}
		return item;
	}

	public static Logger getLog() {
		return log;
	}

}