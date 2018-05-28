package com.zxd.storm.learning.test;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 功能：将spout发过来的手机名称转为大写
 * 
 * @author
 * 
 */
@SuppressWarnings("serial")
public class PhoneTerminalToUpperCaseBolt extends BaseBasicBolt {

	/**
	 * 是bolt组件的业务处理方法，它被worker进程的executor线程不断地调用，每收到一个消息tuple就调用一次
	 * tuple：上一个组件发过来的消息 collector ： 用来发射消息的工具
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		// 从消息中拿到要处理的数据
		// 获取数据的方式一：根据tuple中的field名称来获取
		// String phone = tuple.getStringByField("phone-name");

		// 获取数据的方式二：根据要处理的value在tuple中的脚标来获取
		String phone = tuple.getString(0);

		// 处理数据
		String upperCasePhone = phone.toUpperCase();

		// 把处理结果封装成消息tuple发射出去
		collector.emit(new Values(upperCasePhone));

	}

	/**
	 * 用来声明本组件发出的消息tuple的schema
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("upperphone"));
	}

}
