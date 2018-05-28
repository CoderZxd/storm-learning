package com.zxd.storm.learning.test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * 功能：为手机名称添加日期后缀，并输出结果到外部存储系统
 * 
 * @author
 * 
 */
@SuppressWarnings("serial")
public class PhoneTerminalSuffixBolt extends BaseBasicBolt {

	private FileWriter fw = null;

	// bolt组件的初始化方法，是在bolt组件实例化的时候调用一次
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		try {
			fw = new FileWriter("E:/stormoutput/" + UUID.randomUUID());
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		// 从tuple中拿到数据
		String upperCasePhone = tuple.getString(0);

		// 业务处理
		String result = upperCasePhone +"-"+System.currentTimeMillis();

		// 因为本bolt已经是整个topo的最后一级，所以，处理结果要输出到外部存储系统中
		// 先用一种简单的模式测试一下topo运作流程，所以就选择输出到文件中
		try {
			fw.write(result + "\n");
			fw.flush();

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	//因为这个bolt是最后一个bolt，不需要再往下一个组件发射消息，所以这个消息定义就不需要了
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
