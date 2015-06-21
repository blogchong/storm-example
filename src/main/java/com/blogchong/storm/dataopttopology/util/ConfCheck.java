package com.blogchong.storm.dataopttopology.util;

import com.blogchong.storm.dataopttopology.bolt.FilterBolt;
import com.blogchong.storm.dataopttopology.bolt.MetaBolt;
import com.blogchong.storm.dataopttopology.bolt.MysqlBolt;
import com.blogchong.storm.dataopttopology.spout.MetaSpout;
import java.io.File;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email  blogchong@163.com
 * @QQ_G   191321336
 * @Weixin: blogchong
 * @version 2015年06月07日 上午14:31:25
 * @Des  对配置文件进行动态监测
 */

/**
 * 该函数定时检测配置文件是否发生改变，
 * 实现了Topology的动态配置 即在不重启top的情况下，实现数据处理的动态配置
 */

public class ConfCheck extends Thread {

	private String xmlpath = "xxx.xml";
	private int heartbeat = 1000;
	private String type = "type";

	public ConfCheck(String XmlPath, int HeartBeat, String type) {
		this.xmlpath = XmlPath;
		this.heartbeat = HeartBeat;
		this.type = type;
	}

	public void run() {

        // hash初始值
        long init_time = 0;

		for (int i = 0;; i++) {

			try {

				File file = new File(this.xmlpath);
                // 检查时间戳是否一致
				long lasttime = file.lastModified();

				if (i == 0) {
					init_time = lasttime;
				} else {
					if (init_time != lasttime) {

						init_time = lasttime;

						if (this.type.equals(MacroDef.Thread_type_metaqspout)) {
							MetaSpout.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_filterbolt)) {
							FilterBolt.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_mysqlbolt)) {
							MysqlBolt.isload();
						} else if (this.type
								.equals(MacroDef.Thread_type_metaqbolt)) {
							MetaBolt.isload();
						}
					}
				}

				Thread.sleep(this.heartbeat);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		new ConfCheck("MysqlBolt.xml", 1000, "test").start();
		Thread.sleep(100000);
	}

}
