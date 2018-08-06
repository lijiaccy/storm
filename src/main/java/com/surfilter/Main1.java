package com.surfilter;

import com.surfilter.sy.location.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.net.URISyntaxException;

public class Main1 {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        try {
            //移动
            builder.setSpout("mobile",new MobileSpout());
            builder.setBolt("MobileBolt1", new MobileBolt1()).shuffleGrouping("mobile");
            builder.setBolt("MobileBolt2", new MobileBolt2())//
                    //说明在石宏fieldsGrouping的时候，第二个参数就是制定按照那个一字段来进行分组
                    .fieldsGrouping("MobileBolt1", new Fields("word"));
            builder.setBolt("MobileBolt3",new MobileBolt3()).shuffleGrouping("MobileBolt2");

           //联通
            builder.setSpout("unicom",new UnicomSpout());
            builder.setBolt("UnicomBolt1", new UnicomBolt1()).shuffleGrouping("unicom");
            builder.setBolt("UnicomBolt2", new UnicomBolt2())//
                    //说明在石宏fieldsGrouping的时候，第二个参数就是制定按照那个一字段来进行分组
                    .fieldsGrouping("UnicomBolt1", new Fields("word"));
            builder.setBolt("UnicomBolt3",new UnicomBolt3()).shuffleGrouping("UnicomBolt2");

            //电信
            builder.setSpout("telecom",new TelecomSpout());
            builder.setBolt("TelecomBolt1", new TelecomBolt1()).shuffleGrouping("telecom");
            builder.setBolt("TelecomBolt2", new TelecomBolt2())//
                    //说明在石宏fieldsGrouping的时候，第二个参数就是制定按照那个一字段来进行分组
                    .fieldsGrouping("TelecomBolt1", new Fields("word"));
            builder.setBolt("TelecomBolt3",new TelecomBolt3()).shuffleGrouping("TelecomBolt2");


            Config config = new Config();

            if (args.length>0){
                config.setNumWorkers(Integer.parseInt(args[1]));
                config.setMaxSpoutPending(5000);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            }else {
                String topologyName = Main1.class.getSimpleName();
                StormTopology stormTopology = builder.createTopology();
                LocalCluster lCluster = new LocalCluster();
                lCluster.submitTopology(topologyName, config, stormTopology);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

    }
}
