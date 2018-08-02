package com;

import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AllSpout extends BaseRichSpout {
        private Map conf;
        private TopologyContext context;
        private SpoutOutputCollector collector;

        /**
         * 这是一个生命周期方法，一个SumNumSpout实例只运行一次，主要完成初始化的参数设置
         * @param conf      ---->storm程序以及storm集群相关的配置信息
         * @param context   ---->整个Topology上下文对象，可以通过该context获得相关topology应用属性
         * @param collector ---->主要用于收集数据，并将数据发射到下一个阶段
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        //监听一个目录新文件的产生
        public void nextTuple() {
            /**
             * File directory ----> 要要监控的目录对象
             * String[] extensions  ---->要监控的目录下面以什么结尾(说白了就是扩展名)的文件
             *          注意，写文件扩展名的时候不能写"."
             * boolean recursive    ---->是否递归遍历
             */
            Collection<File> files = FileUtils.listFiles(new File("d:/test/storm"),
                    new String[]{"txt", "log"}, true);
            List<String> lines = null;
            try {
                for (File file : files) {
                    lines = FileUtils.readLines(file, "UTF-8");
                    for (String line : lines) {
//                        System.out.println("spout读取到的内容：" + line);
                        collector.emit(new Values(line));
                    }
                    //读取完成一个文件之后，将其重命名，避免下次再读
                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + "." + System.currentTimeMillis()));
                }
            }catch (Exception e) {
//                e.printStackTrace();//这里就不用输出异常信息了
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }