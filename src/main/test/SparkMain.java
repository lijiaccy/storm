/*
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class SparkMain {
    private static final Pattern SPACE = Pattern.compile(",");
        public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkMain.class);

        public static void main(String[] args) {
            SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("MyAPP");
            JavaSparkContext sc = new JavaSparkContext(conf);
            sc.setLogLevel("INFO");
            JavaRDD<String> lines = sc.textFile("D:\\qh13");
            JavaRDD<String> lines1 = sc.textFile("D:\\new.txt");
//            JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//                public Iterator<String> call(String line) throws Exception {
//                    String aStr[] = line.split("|");
//                    String phone = aStr[3];
//                    String time = aStr[9];
//                    String a1 = aStr[10];
//                    String a2 = aStr[11];
//                    return Arrays.asList(phone,time,a1,a2).iterator();
//                }
//            });

//            List<String> collect = words.collect();
//            System.out.println(collect.size()+"=================");


            JavaRDD<String> union = lines1.union(lines);
            PairFunction<String, String, String> keyData = x -> new Tuple2(x.split("\\|")[3], x);
            JavaPairRDD<String, String> pairs = union.mapToPair(keyData).filter( t -> !t._1.equals("0")).reduceByKey((s, s2) -> s);
            List<Tuple2<String, String>> collect = pairs.collect();
            System.out.println(collect.size());
            Set set = new HashSet();
            for (Tuple2<String, String> p:pairs.collect()) {
                set.add(p._1);
//                System.out.println(p._1);
                if (p._1.equals("12345678901")){
                    System.out.println(p._2);
                }
            }
            System.out.println(set.size());
            System.out.println(collect.size());
        }
}
*/
