package com.surfilter.sy.location;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class TelecomBolt1 extends BaseRichBolt {
//        Logger logger = Logger.getLogger(MobileBolt1.class);
    private static final Logger logger =   Logger.getLogger(TelecomBolt1.class);
        private Map conf;
        private TopologyContext context;
        private OutputCollector collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            logger.info("TelecomBolt1准备");
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("line");
            String[] splits = line.split("\\|");
            if (splits.length != 16){
                return;
            }else if("0".equals(splits[3])){
                return;
            }else{
                collector.emit(new Values(splits[3],splits[6] + "|" +splits[9] + "|" +splits[10] + "|" +splits[11]));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "times"));
        }
    }