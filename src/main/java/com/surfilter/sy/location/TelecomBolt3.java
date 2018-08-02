package com.surfilter.sy.location;

import com.surfilter.util.JedisUtil;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TelecomBolt3 extends BaseRichBolt {
//    Logger logger = Logger.getLogger(MobileBolt3.class);
private static final Logger logger =   Logger.getLogger(TelecomBolt3.class);
        private Map conf;
        private TopologyContext context;
        private OutputCollector collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            logger.info("进入TelecomBolt3");
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        ConcurrentMap map = new ConcurrentHashMap();
        public void execute(Tuple tuple) {
            if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
                if (map.isEmpty()){
                }else {
                    logger.info("开始存入redis");
                    JedisUtil.pip(map,"telecom");
                    logger.info("存入redis结束");
                    map.clear();
                }
            }else {
                logger.info("存入map");
                long start = System.currentTimeMillis();
                String word = tuple.getStringByField("word");
                String times = tuple.getStringByField("times");
                long end = System.currentTimeMillis();
                map.put(word,times);
            }
        }

        //设置10秒发送一次tick心跳
        @SuppressWarnings("static-access")
        @Override
        public Map<String, Object> getComponentConfiguration() {
            Config conf = new Config();
            conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
            return conf;
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }