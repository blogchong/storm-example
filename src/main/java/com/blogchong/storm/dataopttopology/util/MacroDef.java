package com.blogchong.storm.dataopttopology.util;

/**
 * @author blogchong
 * @Blog   www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email  blogchong@163.com
 * @QQ_G   191321336
 * @Weixin: blogchong
 * @version 2015年06月07日 上午11:31:25
 * @Des  全局的一些类似宏定义
 */

public  class MacroDef {

    ////////////////////特殊分割标志//////////////////////
    //分割标志','
	public static final String FLAG_COMMA = ",";
    //分割标志'\t'
	public static final String FLAG_TABS = "\t";
    //分割标志'::'
	public static final String FLAG_COLON = "::";
    //分割标志"\n"
	public final static String FLAG_ROW = "\n";//记录间隔符

    ////////////////////////规则定义/////////////////////
	public static final String RULE_AND = "AND";
	public static final String RULE_OR = "OR";
	public static final String RLUE_REGULAR = "regular";
	public static final String RULE_RANGE = "range";
	public static final String RULE_ROUTINE0 = "routine0";
	public static final String RULE_ROUTINE1 = "routine1";

    /////////////////编码UTF-8///////////////
	public static final String ENCODING = "UTF-8";

    //////////////////XML解析定义///////////////////
	public static final String Parameter = "Parameter";
	//MonitorBoltXml
	public static final String MatchLogic = "MatchLogic";
	public static final String MatchType = "MatchType";
	public static final String MatchField = "MatchField";
	public static final String FieldValue = "FieldValue";
	//MysqlBoltXml
	public static final String Host_port = "Host_port";
	public static final String Database = "Database";
	public static final String Username = "Username";
	public static final String Password = "Password";
	public static final String From = "From";	
	//SpoutXml
	public static final String MetaRevTopic = "MetaRevTopic";
	public static final String MetaZkConnect = "MetaZkConnect";
	public static final String MetaZkRoot = "MetaZkRoot";
	public static final String MetaConsumerConf = "MetaConsumerConf";
	//MetaXml
	public static final String MetaTopic = "MetaTopic";



    ////////////////////调试输出统计/////////////////////
    //MetaSpout调试统计输出
	public final static long SPOUT_DEBUG = 1000;//Tupleͳ�������
	public final static boolean SPOUT_FLAG = false;
    //MetaBolt调试统计输出
	public final static long meta_debug = 1000;//
	public final static boolean meta_flag = false;

    /////////////检测配置文件的HeartBeat心跳间隔(ms)////////////////////
	public final static int  HEART_BEAT = 1000;

    //////////////////监控配置线程定义//////////////////////////
	public final static String Thread_type_metaqspout = "metaqspout";
	public final static String Thread_type_filterbolt = "filterbolt";
	public final static String Thread_type_mysqlbolt = "mysqlbolt";
	public final static String Thread_type_metaqbolt = "metaqbolt";
	
}
