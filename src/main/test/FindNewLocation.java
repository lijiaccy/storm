/*
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

public class FindNewLocation
{

    static JavaPairDStream<String, String> streamAll =null;
    public static void main(String[] args) throws InterruptedException {

        //注意本地调试，master必须为local[n],n>1,表示一个线程接收数据，n-1个线程处理数据
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("streaming");
        JavaSparkContext sc = new JavaSparkContext(conf);
//设置日志运行级别
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));
//创建一个将要连接到hostname:port 的离散流
//        JavaReceiverInputDStream<String> lines =ssc.socketTextStream("slvae", 9999);
        JavaDStream<String> lines = ssc.textFileStream("hdfs://slvae:9000/hello");

//        JavaPairDStream<String, Integer> counts =
//                lines.flatMap(x->Arrays.asList(x.split(" ")).iterator())
//                        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
//                        .reduceByKey((x, y) -> x + y);
// 在控制台打印出在这个离散流（DStream）中生成的每个 RDD 的前十个元素
//        counts.print();
//
//        JedisUtil.setStringValue("count",counts.count().toString());

        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String x) throws Exception {
                return new Tuple2(x.split("\\|")[3], x);
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
                String time1 = s.split("\\|")[9];
                String time2 = s2.split("\\|")[9];
                if (time1.compareTo(time2)>=0){
                    return s;
                }else {
                    return s2;
                }
            }
        });

        stream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                List<Tuple2<String, String>> collect = rdd.collect();
                System.out.println(collect.size());
                for (Tuple2<String, String> p:collect) {
                    JedisUtil.setStringValue(p._1,p._2);
                }
                System.out.println(collect.size());
            }

        });

        ssc.start();
        ssc.awaitTermination();

    }
}
*/
