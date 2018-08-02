package com.surfilter.sy.location;

import com.surfilter.util.JedisUtil;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UnicomBolt2 extends BaseRichBolt {
//        Logger logger = Logger.getLogger(MobileBolt2.class);
    private static final Logger logger =   Logger.getLogger(UnicomBolt2.class);
        private Map conf;
        private TopologyContext context;
        private OutputCollector collector;
        ConcurrentHashMap<String,String> map = new ConcurrentHashMap<String,String>();

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            logger.info("UnicomBolt2准备");
            this.conf = conf;
            this.context = context;
            this.collector = collector;
            if (map.size() == 0){
                map = JedisUtil.getpip("unicom");
                logger.info("加载了====="+map.size()+"=====条记录");
            }
        }

        public void execute(Tuple tuple) {
                String word = tuple.getStringByField("word");
                String times = tuple.getStringByField("times");
                if (!map.containsKey(word)){
                    map.put(word,times);
                    collector.emit(new Values(word,times));
                }else {
                    String s = map.get(word);
                    String time1 = s.split("\\|")[1];
                    String time2 = times.split("\\|")[1];
                    if (time1.compareTo(time2) < 0) {
                        map.put(word,times);
                        collector.emit(new Values(word,times));
                    }
                }
            }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","times"));
        }
    }