package com.zxd.storm.learning.test;

import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * spout组件是整个topo的源组件
 * 
 * @author
 * 
 */
public class PhoneTerminalSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	// 模拟一些数据
	String[] phones = { "iphone", "xiaomi", "moto", "sunsumg", "mate", "meizu", "chuizi" };

	/**
	 * 消息的处理方法，不断地往后续流程发送消息，调用一次发送一个消息tuple，会连续不断地被worker进程的excutor线程调用
	 */
	@Override
	public void nextTuple() {

		// 模拟从外部数据源获取数据
		Utils.sleep(500);
		int index = new Random().nextInt(7);
		String phone = phones[index];
		

		// 将拿到的数据封装为tuple发送出去
		// 一个tuple中可以封装多个数据（类似list的元素）
		// collector.emit(new Values("value1","value2","value3"));
		collector.emit(new Values(phone));

	}

	/**
	 * 是组件的初始化方法，类似于mapper里面的setup方法
	 */
	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	/**
	 * 定义本组件发出的tuple的schema —— 有几个字段，每个字段的字段名称
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		// 为发出去的tuple中的每一个字段定义一个名字
		// declarer.declare(new Fields("field1","field2","field3"));

		declarer.declare(new Fields("phone-name"));

	}

	/**
	 * test pattern
	 * @param args
	 */
	public static void main(String[] args){
		Pattern pattern = Pattern.compile("#\\{[^{}]+\\}");
		String ss = "{'name':'Hello #{name} #{value}','value':'#{{name}}'}";
		Matcher matcher = pattern.matcher(ss);
		while (matcher.find()){
			System.out.println(matcher.group());
			int i = matcher.groupCount();
			System.out.println("========="+i);
		}
	}

}
