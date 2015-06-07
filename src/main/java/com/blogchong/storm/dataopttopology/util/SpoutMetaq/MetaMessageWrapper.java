package com.blogchong.storm.dataopttopology.util.SpoutMetaq;

import java.util.concurrent.CountDownLatch;
import com.taobao.metamorphosis.Message;

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

public final class MetaMessageWrapper {

	public final Message message;
	public final CountDownLatch latch;
	public volatile boolean success = false;

	public MetaMessageWrapper(final Message message) {
		super();
		this.message = message;
		this.latch = new CountDownLatch(1);
	}
}