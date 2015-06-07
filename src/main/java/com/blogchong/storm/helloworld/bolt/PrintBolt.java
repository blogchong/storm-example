package com.blogchong.storm.helloworld.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @Author: blogchong
 * @Blog: www.blogchong.com
 *  @米特吧大数据论坛　www.mite8.com
 * @Mailbox: blogchong@163.com
 * @QQGroup: 191321336
 * @Weixin: blogchong
 * @Data: 2015/4/7
 * @Describe: 打印接受的数据
 */

@SuppressWarnings("serial")
public class PrintBolt extends BaseBasicBolt {

    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String mesg = input.getString(0);
            if (mesg != null)
                System.out.println(mesg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
