package com.blogchong.storm.dataopttopology.spout;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.blogchong.storm.dataopttopology.util.MacroDef;

/**
 * @author blogchong
 * @version 2015年06月07日 上午11:31:25
 * @Blog www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email blogchong@163.com
 * @QQ_G 191321336
 * @Weixin: blogchong
 * @Des Spout数据源，从log文件中直接读取数据
 */

public class ReadLogSpout implements IRichSpout {

	private SpoutOutputCollector collector;
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
		
		String file = "domain.log";

		try {

			this.fis = new FileInputStream(file);
			this.isr = new InputStreamReader(fis, MacroDef.ENCODING);
			this.br = new BufferedReader(isr);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void nextTuple() {
		String str = "";
		try {
			while ((str = this.br.readLine()) != null) {
				this.collector.emit(new Values(str));
				Thread.sleep(100);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
