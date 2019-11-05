package com.bigdata;

import redis.clients.jedis.Jedis;

public class RedisUtils {

	public static Jedis getJedis(String host){


		Jedis jedis = new Jedis(host,6379);

		return jedis;
	}
}
