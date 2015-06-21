package com.blogchong.storm.dataopttopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.blogchong.storm.dataopttopology.util.ConfCheck;
import com.blogchong.storm.dataopttopology.util.MacroDef;
import com.blogchong.storm.dataopttopology.xml.FilterXml;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author blogchong
 * @version 2015年06月07日 上午14:31:25
 * @Blog www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email blogchong@163.com
 * @QQ_G 191321336
 * @Weixin: blogchong
 * @Des 过滤Bolt，进行数据的正则过滤，范围过滤以及普通字符串过滤
 */

@SuppressWarnings("serial")
public class FilterBolt implements IRichBolt {

	private OutputCollector collector;

    //是否加载配置标志位
	private static boolean flag_load = false;

	private long register = 0;

    //默认参数~~
	String monitorXml = "Filter.xml";
    // 参数判空标志
	private boolean flag_par = true;
    // 匹配条件间的逻辑关系
	String MatchLogic = "AND";
    // !--匹配类型列表
	String MatchType = "regular::range::routine0";
    // !--匹配字段列表-
	String MatchField = "1::2::5";
    // !--字段值列表-
	String FieldValue = ".*baidu.*::1000,2000::ina";

	public FilterBolt(String MonitorXML) {

		if (MonitorXML == null) {
			flag_par = false;
		} else {
			this.monitorXml = MonitorXML;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		System.out.println("FilterBolt	--	Start!");
		this.collector = collector;
				
		if (!this.flag_par) {
			System.out
					.println("MetaSpout-- Erre: can't get the path of Spout.xml!");
		} else {
            // 调用检测线程
			new ConfCheck(this.monitorXml, MacroDef.HEART_BEAT,
					MacroDef.Thread_type_filterbolt).start();
		}

	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);

		if (!this.flag_par) {
			System.out
					.println("FilterBolt-- Erre: can't get the path of Filter.xml!");
		} else {

            // 检测配置文件是否更改
			if (!flag_load ) {
                // 配置文件发生更改则进行加载参数操作
				Loading();
				if (register != 0) {
					System.out.println("FilterBolt-- Conf Change: "
							+ this.monitorXml);
				} else {
					System.out.println("FilterBolt-- Conf Loaded: "
							+ this.monitorXml);
				}
			}

			boolean moni = Monitor(str, this.MatchLogic, this.MatchType,
					this.MatchField, this.FieldValue);
			if (moni) {
				this.collector.emit(new Values(str));
			}

            this.collector.ack(input);
		}
	}

    //更改标志位
	public static void isload() {
		flag_load = false;
	}

    // 加载参数操作
	public void Loading() {
        // 从conf中获取参数
		System.out.println("FilterXml:     " + this.monitorXml);

		new FilterXml(this.monitorXml).read();
		this.MatchLogic = FilterXml.MatchLogic;
		this.MatchType = FilterXml.MatchType;
		this.MatchField = FilterXml.MatchField;
		this.FieldValue = FilterXml.FieldValue;
		flag_load = true;
	}

	private boolean Monitor(String str, String logic, String type,
			String field, String value) {

		String[] types = type.split(MacroDef.FLAG_COLON);
		String[] fields = field.split(MacroDef.FLAG_COLON);
		String[] values = value.split(MacroDef.FLAG_COLON);

		int flag_init = types.length;
		int flag = 0;

		if (logic.equals(MacroDef.RULE_AND)) {
			for (int i = 0; i < flag_init; i++) {
				if (types[i].equals(MacroDef.RLUE_REGULAR)) {
					boolean regu = regular(str, fields[i], values[i]);
					if (regu) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_RANGE)) {
					boolean ran = range(str, fields[i], values[i]);
					if (ran) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE0)) {
					boolean rou0 = routine0(str, fields[i], values[i]);
					if (rou0) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE1)) {
					boolean rou1 = routine1(str, fields[i], values[i]);
					if (rou1) {
						flag++;
					}
				}
			}

			if (flag == flag_init) {
				return true;
			} else {
				return false;
			}
			
		} else if (logic.equals(MacroDef.RULE_OR)) {
			
			for (int i = 0; i < flag_init; i++) {
				if (types[i].equals(MacroDef.RLUE_REGULAR)) {
					boolean regu = regular(str, fields[i], values[i]);
					if (regu) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_RANGE)) {
					boolean ran = range(str, fields[i], values[i]);
					if (ran) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE0)) {
					boolean rou0 = routine0(str, fields[i], values[i]);
					if (rou0) {
						flag++;
					}
				} else if (types[i].equals(MacroDef.RULE_ROUTINE1)) {
					boolean rou1 = routine1(str, fields[i], values[i]);
					if (rou1) {
						flag++;
					}
				}
			}
			if (flag != 0) {
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

    // 正则匹配判断
	private boolean regular(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);

		Pattern p = Pattern.compile(value);
		Matcher m = p.matcher(strs[Integer.parseInt(field) - 1]);
		boolean result = m.matches();

		if (result) {
			return true;
		} else {
			return false;
		}
	}

    // 范围匹配
	private boolean range(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);
		String[] values = value.split(MacroDef.FLAG_COMMA);

		int strss = Integer.parseInt(strs[Integer.parseInt(field) - 1]);

		if (values.length == 1) {
			if (strss > Integer.parseInt(values[0])) {
				return true;
			} else {
				return false;
			}
		} else if (values.length == 2 && values[0].length() == 0) {
			if (strss < Integer.parseInt(values[1])) {
				return true;
			} else {
				return false;
			}
		} else if (values.length == 2 && values[0].length() != 0) {
			if (strss > Integer.parseInt(values[0])
					&& strss < Integer.parseInt(values[1])) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

    // 常规模糊匹配
	private boolean routine0(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);
		String strss = strs[Integer.parseInt(field) - 1];

		if (strss.contains(value)) {
			return true;
		} else {
			return false;
		}
	}

    // 常规完全匹配
	private boolean routine1(String str, String field, String value) {
		String[] strs = str.split(MacroDef.FLAG_TABS);
		String strss = strs[Integer.parseInt(field) - 1];

		if (strss.equals(value)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void cleanup() {

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
