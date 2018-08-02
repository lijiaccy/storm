/*
import com.surfilter.util.PropertiesUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Properties;

public class Location1 {
        private static Logger logger = LoggerFactory.getLogger(Location1.class);
    public static void main(String[] args) {
        JavaStreamingContext context = JavaStreamingContext.getOrCreate("hdfs://slvae:9000/lijia",findlocation());
        context.start();
        try {
            context.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


        public static Function0<JavaStreamingContext> findlocation(){
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
            }).reduceByKey((Function2<String, String, String>) (x, s2) -> {
                String time1 = x.split("\\|")[0];
                String time2 = s2.split("\\|")[0];
                if (time1.compareTo(time2) > 0) {
                    return x;
                } else {
                    return s2;
                }
            });

            stream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
                @Override
                public void call(JavaPairRDD<String, String> rdd) throws Exception {
                    List<Tuple2<String, String>> collect = rdd.collect();
                    logger.info("写入文件开始" + System.currentTimeMillis());
                    rdd.cache();
                    rdd.checkpoint();
//                initFile2(collect);
                    System.out.println(collect.size());
                    logger.info("写入文件结束" + System.currentTimeMillis());
                    System.out.println(collect.size());
                }
            });
            return new Function0<JavaStreamingContext>() {
                @Override
                public JavaStreamingContext call() throws Exception {
                    return ssc;
                }
            };

    }


}
*/
