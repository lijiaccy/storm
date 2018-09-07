package com.surfilter.sy.location;

import com.surfilter.util.JedisUtil;
import com.surfilter.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MobileBolt11 extends BaseRichBolt {
//    Logger logger = Logger.getLogger(MobileBolt3.class);
private static final Logger logger =   Logger.getLogger(MobileBolt11.class);
        private Map conf;
        private TopologyContext context;
        private OutputCollector collector;
        FileSystem hdfs;
        FSDataOutputStream os;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            logger.info("进入MobileBolt3");
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        public void openHDFSFile() throws IOException {
            String filename = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())+"-"+(int)(Math.random()*1000)+".txt";
            Properties prop = PropertiesUtil.loadProperties();
            String uri = PropertiesUtil.getString(prop, "mobile.uri");
            String des = PropertiesUtil.getString(prop, "mobile.des");
            String newFile = uri+des+filename;
            Configuration config = new Configuration();
            hdfs = FileSystem.get(URI.create(newFile), config);
            os = hdfs.create(new Path(newFile));
        }

        public void closeHDFSFile() throws IOException {
            os.close();
            hdfs.close();
        }

        public boolean flag =true;
        public void execute(Tuple tuple) {
            if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
                try {
                    flag =true;
                    closeHDFSFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }else {
                String line = tuple.getStringByField("line");
                if ("".equals(line) || null == line){
                    return;
                }
                if (flag){
                    try {
                        flag =false;
                        openHDFSFile();
                        os.write(line.getBytes("UTF-8"));
                        return;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }else {
                    try {
                        os.write(line.getBytes("UTF-8"));
                        return;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

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