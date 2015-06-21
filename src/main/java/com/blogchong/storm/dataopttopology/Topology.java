package com.blogchong.storm.dataopttopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.blogchong.storm.dataopttopology.bolt.FilterBolt;
import com.blogchong.storm.dataopttopology.bolt.MetaBolt;
import com.blogchong.storm.dataopttopology.bolt.MysqlBolt;
import com.blogchong.storm.dataopttopology.bolt.PrintBolt;
import com.blogchong.storm.dataopttopology.spout.MetaSpout;
import com.blogchong.storm.dataopttopology.spout.ReadLogSpout;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @米特吧大数据论坛 www.mite8.com
 * @email  blogchong@163.com
 * @QQ_G   191321336
 * @Weixin: blogchong
 * @version 2015年06月07日 上午14:31:25
 * @Des  数据源Spout，从metaq中消费数据/从文本中读取数据
 */

/**
 * 主类，只要spout、bolt中有的，可以随意组合top
 */

public class Topology {

    // 实例化TopologyBuilder类。
	private static TopologyBuilder builder = new TopologyBuilder();

	public static void main(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		Config config = new Config();

        // 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
//		builder.setSpout("spout", new ReadLogSpout(), 1);
        builder.setSpout("spout", new MetaSpout("MetaSpout.xml"), 1);

        // 创建filter过滤节点
		builder.setBolt("filter", new FilterBolt("FilterBolt.xml"), 1)
				.shuffleGrouping("spout");

        // 创建mysql数据存储节点
		builder.setBolt("mysql", new MysqlBolt("MysqlBolt.xml"), 1)
				.shuffleGrouping("filter");

        //metaq回写节点
        builder.setBolt("meta", new MetaBolt("MetaBolt.xml"), 1)
                .shuffleGrouping("filter");

		builder.setBolt("print", new PrintBolt(), 1).shuffleGrouping("filter");

		config.setDebug(false);

		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopology(args[0], config,
					builder.createTopology());
		} else {
            // 这里是本地模式下运行的启动代码。
			config.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("simple", config, builder.createTopology());
		}

	}

}
