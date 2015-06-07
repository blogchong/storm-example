package com.blogchong.storm.dataopttopology.util.SpoutMetaq;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.blogchong.storm.dataopttopology.util.MacroDef;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email  blogchong@163.com
 * @QQ_G   191321336
 * @Weixin: blogchong
 * @version 2015年06月07日 上午14:31:25
 * @Des Meta消息封装
 */

//规范化输出，定义编码
public class StringScheme implements Scheme {

	public List<Object> deserialize(byte[] bytes) {

		try {

            //数据的编码定义
			return new Values(new String(bytes, MacroDef.ENCODING));

		} catch (UnsupportedEncodingException e) {

			throw new RuntimeException(e);

		}
	}

	public Fields getOutputFields() {
		return new Fields("str");
	}
}