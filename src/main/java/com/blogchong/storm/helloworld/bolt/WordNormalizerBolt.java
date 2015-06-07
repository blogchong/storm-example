package com.blogchong.storm.helloworld.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @Author: blogchong
 * @Blog: www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @Mailbox: blogchong@163.com
 * @QQGroup: 191321336
 * @Weixin: blogchong
 * @Data: 2015/4/7
 * @Describe: 消息标准化处理
 */

//将消息标准化，
@SuppressWarnings("serial")
public class WordNormalizerBolt implements IRichBolt {

    private OutputCollector outputCollector;

    //bolt初始化方法
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        outputCollector = collector;
    }

    //执行订阅的Tuple逻辑过程的方法
    public void execute(Tuple input) {

        String sentence = input.getString(0);
        String[] words = sentence.split(" ");

        for (String word : words) {
            outputCollector.emit(new Values(word));
        }

    }

    public void cleanup() {
    }

    //字段声明
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
