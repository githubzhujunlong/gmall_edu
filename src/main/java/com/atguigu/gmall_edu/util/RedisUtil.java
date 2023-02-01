package com.atguigu.gmall_edu.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private static JedisPool pool;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(5);
        config.setMinIdle(2);
        config.setMaxWaitMillis(60 * 1000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestOnCreate(true);

        pool = new JedisPool(config, "hadoop102", 6379);
    }

    public static Jedis getRedisClient(){
        Jedis jedis = pool.getResource();
        jedis.select(6);
        return jedis;
    }
}
