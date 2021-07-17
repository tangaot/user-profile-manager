package com.atguigu.userprofile.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

public class RedisUtil {

    public static void main(String[] args) {
        //Jedis jedis = new Jedis("bigdata01",6379);
        Jedis jedis = RedisUtil.getJedis();

        //jedis.auth("123");
        System.out.println(jedis.ping());

        jedis.set("k1000","v1000");

        System.out.println(jedis.get("k1000"));

        Map<String, String> stringStringMap = jedis.hgetAll("user:101");
        System.out.println(stringStringMap);

        Set<Tuple> article_topn = jedis.zrevrangeWithScores("article_topn", 0, -1);
        System.out.println(article_topn);

        jedis.close();

    }

    static  JedisPool  jedisPool =null;


    public static Jedis getJedis() {
      if(jedisPool==null){
          JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
          jedisPoolConfig.setMaxTotal(200); // 最大连接数
          jedisPoolConfig.setMaxIdle(30);// 最多维持30
          jedisPoolConfig.setMinIdle(10);// 至少维持10
          jedisPoolConfig.setBlockWhenExhausted(true);
          jedisPoolConfig.setMaxWaitMillis(5000);
          jedisPoolConfig.setTestOnBorrow(true);

          jedisPool = new JedisPool(jedisPoolConfig,"bigdata01",6379,60000, "123");

      }
       return   jedisPool.getResource();
    }
}
