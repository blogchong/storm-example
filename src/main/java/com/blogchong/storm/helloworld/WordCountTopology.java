package com.blogchong.storm.helloworld;

import backtype.storm.tuple.Fields;
import com.blogchong.storm.helloworld.bolt.PrintBolt;
import com.blogchong.storm.helloworld.bolt.WordCountBolt;
import com.blogchong.storm.helloworld.bolt.WordNormalizerBolt;
import com.blogchong.storm.helloworld.spout.RandomSentenceSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * @Author: blogchong
 * @Blog: www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @Mailbox: blogchong@163.com
 * @QQGroup: 191321336
 * @Weixin: blogchong
 * @Data: 2015/4/7
 * @Describe: 启动主类，拓扑构建
 */

public class WordCountTopology {

    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) {

        Config config = new Config();

        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
        builder.setBolt("WordNormalizer", new WordNormalizerBolt(), 2).shuffleGrouping(
                "RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt(), 2).fieldsGrouping("WordNormalizer",
                new Fields("word"));
        builder.setBolt("Print", new PrintBolt(), 1).shuffleGrouping(
                "WordCount");

        config.setDebug(false);

        //通过是否有参数来控制是否启动集群，或者本地模式执行
        if (args != null && args.length > 0) {
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config,
                        builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", config, builder.createTopology());
        }
    }
}
