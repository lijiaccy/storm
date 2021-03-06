/*
package com;


import com.surfilter.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.scheduler.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import scala.Tuple2;

import java.util.*;


*/
/**
 * cat 2.txt | ./redis-cli -h 172.30.154.123 -p 6379 -a 123456 --pipe
 *//*



public class FindNewLocation {

    private static Logger logger = LoggerFactory.getLogger(FindNewLocation.class);

    public static void findlocation() throws InterruptedException {
        logger.info("进入任务");
        Properties prop = PropertiesUtil.loadProperties(PropertiesUtil.DEFAULT_CONFIG);
        String address = PropertiesUtil.getString(prop, "hdfs.location");
        int timeinterval = PropertiesUtil.getInt(prop, "time.interval");

        SparkConf conf = new SparkConf().setMaster("local[6]").setAppName("location");
//        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(timeinterval));
        ssc.checkpoint("hdfs://slvae:9000/lijia");

        logger.info("开始读取hdfs");
        JavaDStream<String> lines = ssc.textFileStream(address);
        logger.info("成功连接hdfs");
        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String x) throws Exception {
                if (x.split("\\|").length != 16) return new Tuple2<>("0", "errorData");
                return new Tuple2(x.split("\\|")[3], x.split("\\|")[9] + "|" + x.split("\\|")[10] + "|" + x.split("\\|")[11]);
            }
        };

        JavaPairDStream<String, String> stream = lines.mapToPair(keyData).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> t) throws Exception {
                return !t._1.equals("0");
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                String time1 = s.split("\\|")[0];
                String time2 = s2.split("\\|")[0];
                if (time1.compareTo(time2) > 0) {
                    return s;
                } else {
                    return s2;
                }
            }
        });


        stream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                List<Tuple2<String, String>> collect = rdd.collect();
                logger.info("写入文件开始" + System.currentTimeMillis());
//                initFile2(collect);
                logger.info("写入文件结束" + System.currentTimeMillis());
                System.out.println(collect.size());
            }
        });
        ssc.start();
        ssc.awaitTermination();
        }

//    public static void initFile2(List<Tuple2<String, String>> p){
//        if (p.size() ==0){
//
//        }else{
//                Jedis jedis = new Jedis("172.30.154.123",6379);
//                jedis.auth("123456");
//                Pipeline pipelined = jedis.pipelined();
//                long start = System.currentTimeMillis();
//                for(int i=0 ;i < p.size();i++){
//                    pipelined.set(p.get(i)._1,p.get(i)._2);
//                }
//                pipelined.syncAndReturnAll();
//                long emd = System.currentTimeMillis();
//                System.out.println(emd -start);
//                jedis.disconnect();
//            }
//        }

}







*/
