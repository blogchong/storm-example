package com.blogchong.storm.dataopttopology.sourcedata;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.utils.ZkUtils;
import java.util.Random;

/**
 * @author blogchong
 * @version 2015年06月15日 上午19:31:25
 * @Blog www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email blogchong@163.com
 * @QQ_G 191321336
 * @Weixin: blogchong
 * @Des 构造一个随机的域名交易信息源，以生产到metaq中
 */

//该的目的是构造一个随机的domain数据集
public class DataProDucer {

    private static String topic = "blogchong-test";
    private static String zkRoot = "/meta";
    private static String zkConnect = "192.168.5.240:2181";

    private static  MetaClientConfig metaClientConfig;
    private transient static MessageSessionFactory sessionFactory;
    private transient static MessageProducer messageProducer;
    private transient static SendResult sendResult;

    public static void main(String[] args) throws Exception {

        Random random = new Random();

        //构造一个随机记录
        String[] net0 = {"baidu", "hitwh", "google", "jikexueyuan", "hadoop",
                "storm", "blogchong", "cooje", "mite8", "mygod"};
        String[] net1 = {"com", "net", "cn", "edu", "tv", "org", "us", "jp","club" ,"pub",
                "rec", "info"};
        String[] times = {"2000", "2001", "2002", "2005", "2007", "2010",
                "2011", "2012", "2013", "1998", "2014", "2015"};
        String[] value = {"1326", "1446", "1401", "1202", "1871", "2000",
                "122", "23000", "400", "240", "8888", "100000", "34123"};
        String[] validity = {"3", "5", "20", "100", "32", "12", "50", "1",
                "23", "45", "200"};
        String[] seller = {"Huang", "Lina", "James", "Gale", "Kathryn",
                "Acong", "Green", "Facke", "Nina", "Litao", "Pony", "Blogchong", "Mite"};

        ZkUtils.ZKConfig zkconf = new ZkUtils.ZKConfig();
        zkconf.zkConnect = zkConnect;
        zkconf.zkRoot = zkRoot;
        MetaClientConfig metaconf = new MetaClientConfig();
        metaconf.setZkConfig(zkconf);
        metaClientConfig = metaconf;
        sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        messageProducer = sessionFactory.createProducer();
        messageProducer.publish(topic);


        while (true) {
            // 构造域名
            String net = "www." + net0[random.nextInt(net0.length)] + "."
                    + net1[random.nextInt(net1.length)];
            String records = net + "\t" + value[random.nextInt(value.length)] + "\t"
                    + times[random.nextInt(times.length)] + "\t"
                    + validity[random.nextInt(validity.length)] + "\t"
                    + seller[random.nextInt(seller.length)];

            sendResult = messageProducer
                    .sendMessage(new Message(topic, (records).getBytes()));

            if (sendResult.isSuccess()) {
                System.out.println("消息：[" + records + "] 发送成功！");
            } else {
                System.err.println("消息：[" + records + "] 发送失败！");
            }

            Thread.sleep(100);
        }

    }

}
