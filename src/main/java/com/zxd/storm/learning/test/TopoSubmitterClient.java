package com.zxd.storm.learning.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import clojure.main;

/**
 * 功能：构造一个Topology对象，并向storm集群提交
 * 
 * @author
 * 
 */
public class TopoSubmitterClient {

	public static void main(String[] args) throws Exception {

		// 先获得一个Topology的构建器
		TopologyBuilder builder = new TopologyBuilder();

		// 指定topo所用的spout组件类，参数1：spout的id 参数2：spout的实例对象
		builder.setSpout("phone-spout", new PhoneTerminalSpout());

		// 指定topo所用的第一个bolt组件，同时指定本bolt的消息流是从哪个组件流过来的
		builder.setBolt("upper-bolt", new PhoneTerminalToUpperCaseBolt()).shuffleGrouping("phone-spout");

		// 指定topo所用的第二个bolt组件，同时指定本bolt的消息流是从哪个组件流过来的
		builder.setBolt("suffix-bolt", new PhoneTerminalSuffixBolt()).shuffleGrouping("upper-bolt");

		// 使用builder来生成一个Topology对象
		StormTopology phoneTopo = builder.createTopology();

		// 将phoneTopo提交集群运行
		Config config = new Config();
		// 指定让storm集群为我们这个Topology分配6个worker来执行
		config.setNumWorkers(6);

//		if (args.length > 0) {
//			// 通过StormSubmitter向storm集群提交
//			StormSubmitter.submitTopology(args[0], config, phoneTopo);
//		} else {
			// storm可以通过创建一个localCluster，来进行本地模拟运行测试
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("phone-topo", config, phoneTopo);

//		}

	}

}
