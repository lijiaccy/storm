package com;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class test {

    public static void main(String[] args) {
        ConcurrentMap map = new ConcurrentHashMap();
        long start = System.currentTimeMillis();

        //连接redis服务器，localhost:6379
        Jedis redis = new Jedis("172.30.154.123", 6379);
        redis.auth("123456");
        // 获取所有key

        redis.scan("0");
        Set<String> keySet = redis.keys("*");
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            String value = redis.get(key);
            map.put(key,value);
        }
        long end = System.currentTimeMillis();
        // 计算耗时
        System.out.println("Query " + map.size()+ " pairs takes " + (end - start) + " millis");
        redis.close();


    }
    
}
