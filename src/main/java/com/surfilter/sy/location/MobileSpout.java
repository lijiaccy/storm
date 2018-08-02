package com.surfilter.sy.location;

import com.surfilter.util.HUtil;
import com.surfilter.util.PropertiesUtil;
import io.netty.util.internal.ConcurrentSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

public class MobileSpout extends BaseRichSpout implements Serializable {
//    Logger logger = Logger.getLogger(MobileSpout.class);
private static final Logger logger =   Logger.getLogger(MobileSpout.class);

    ConcurrentSet set = new ConcurrentSet();
    Properties prop = PropertiesUtil.loadProperties();
    String uri = PropertiesUtil.getString(prop, "mobile.uri");
    String des = PropertiesUtil.getString(prop, "mobile.des");
    SpoutOutputCollector collector;

    public MobileSpout() throws IOException, URISyntaxException {
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        FileSystem fs = null;
        logger.info("开始读取hdfs数据");
        Configuration config = new Configuration();
        try {
            fs = FileSystem.get(new URI(uri), config);
            ArrayList<Path> list = HUtil.listFilesByModificationTime(fs, new Path(uri+des), set);
            logger.info("hdfs中有"+list.size()+"条数据");
            if(list.size()>0){
                for (Path p : list) {
                    String s = p.getName();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri+des+s))));
                    String data = null;
                    while ((data = reader.readLine())!=null){
                        collector.emit(new Values(data),data);
                    }
                    fs.deleteOnExit(new Path(uri+des+s));
                    System.out.println("开始删除hdfs中的文件"+uri+des+s);
                    set.remove(s);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void fail(Object msgId) {
        logger.info("失败数据："+msgId);
        super.fail(msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }


}
