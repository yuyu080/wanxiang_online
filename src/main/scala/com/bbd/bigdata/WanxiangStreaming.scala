package com.bbd.bigdata

import java.util.Properties
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import org.apache.flink.streaming.api.scala._


object WanxiangStreaming {
  def cypher_generate(message : String) : Array[String] = {

    return null;
  }
  def cypher_fetch(message : String )  {
    /*
    *调用函数，输入一条数据，返回包含cypher语句或者数据异常信息
    * if cypher: 直接连接neo4j执行
    * if Error message: 写到异常数据kafka topic 等待后续处理
    * */



  }
  def main(args: Array[String]) {
    /*entrance,define and init streaming execution environment configuration */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(100)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setRestartStrategy(RestartStrategies.fallBackRestart())
    //flink exactly_once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //定义kafka 配置,KAFKA_BROKER和消费组
    //val ZOOKEEPER_HOST = "10.28.40.11:2181,10.28.40.12:2181,10.28.40.13:2181,10.28.40.14:2181,10.28.40.15:2181"   //zk的地址，下面的是kafka的地址
    val KAFKA_BROKER = "10.28.40.11:9092,10.28.40.12:9092,10.28.40.13:9092,10.28.40.14:9092,10.28.40.15:9092"
    val TRANSACTION_GROUP = "bbd_wanxiang_20171010"
    //初始化kafka
    val kafkaProps = new Properties()
    //kafkaProps.load(ClassLoader.getSystemResourceAsStream("consumer.properties"))
    //kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    //kafkaProps.setProperty("auto.offset.reset","earliest")
    //env.getConfig.addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class)

    val text = env.addSource(
      new FlinkKafkaConsumer010[String](("wanxiang_canal_20170919"), new SimpleStringSchema(), kafkaProps)).map(
      x => x.trim()
      )
    //streamingMessage.addSink()
    text.print()
    env.execute("Wanxiang streaming data process")
  }

}
