package com.blogchong.storm.dataopttopology.xml;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import com.blogchong.storm.dataopttopology.util.MacroDef;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @米特吧大数据论坛　 www.mite8.com
 * @email  blogchong@163.com
 * @QQ_G   191321336
 * @Weixin: blogchong
 * @version 2015年06月07日 上午12:31:25
 * @Des  数据接入Spout接口的xml配置读取接口
 */

public class SpoutXml {

    // xml路径
	private static String fd;
    // MetaBolt参数
    // !--MetaQ消息队列--
	public static String MetaRevTopic;
    // !--MetaQ服务地址--
	public static String MetaZkConnect;
    // !--MetaQ服务路径--
	public static String MetaZkRoot;
    // !--MetaQ消费者组ID--
	public static String MetaConsumerConf;

	public SpoutXml(String str) {
		this.fd = str;
	}

	public void read() {
		try {
			File file = new File(this.fd);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);

			NodeList nl = doc.getElementsByTagName(MacroDef.Parameter);

			Element e = (Element) nl.item(0);

			MetaRevTopic = e.getElementsByTagName(MacroDef.MetaRevTopic).item(0)
					.getFirstChild().getNodeValue();
			
			MetaZkConnect = e.getElementsByTagName(MacroDef.MetaZkConnect).item(0)
					.getFirstChild().getNodeValue();
			
			MetaZkRoot = e.getElementsByTagName(MacroDef.MetaZkRoot).item(0)
					.getFirstChild().getNodeValue();
			
			MetaConsumerConf = e.getElementsByTagName(MacroDef.MetaConsumerConf)
					.item(0).getFirstChild().getNodeValue();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
