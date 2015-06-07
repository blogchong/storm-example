package com.blogchong.storm.helloworld.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.blogchong.storm.helloworld.util.MapSort;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: blogchong
 * @Blog: www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @Mailbox: blogchong@163.com
 * @QQGroup: 191321336
 * @Weixin: blogchong
 * @Data: 2015/4/7
 * @Describe: 单词统计，并且实时获取词频前N的发射出去
 */

@SuppressWarnings("serial")
public class WordCountBolt implements IRichBolt {

    Map<String, Integer> counters;
    private OutputCollector outputCollector;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        outputCollector = collector;
        counters = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
        String str = input.getString(0);

        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        //我们只取词频最大的前十个
        int num = 8;
        int length = 0;

        //使用工具类MapSort对map进行排序
        counters = MapSort.sortByValue(counters);

        if (num < counters.keySet().size()) {
            length = num;
        } else {
            length = counters.keySet().size();
        }

        String word = null;

        //增量统计
        int count = 0;
        for (String key : counters.keySet()) {

            //获取前N个，推出循环
            if (count >= length) {
                break;
            }

            if (count == 0) {
                word = "[" + key + ":" + counters.get(key) + "]";
            } else {
                word = word + ", [" + key + ":" + counters.get(key) + "]";
            }
            count++;
        }

        word = "The first " + num + ": " + word;
        outputCollector.emit(new Values(word));

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}