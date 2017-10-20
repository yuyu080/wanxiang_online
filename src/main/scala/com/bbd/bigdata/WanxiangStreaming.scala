package com.bbd.bigdata

import java.util.Properties
import com.bbd.bigdata.WanxiangSinkToNeo4j.WanxiangSinkToNeo4j
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._


object WanxiangStreaming {
  def main(args: Array[String]) {
    /*
    * 程序入口，定义streaming执行环境的参数
    * 定义source，sink相关配置，以及整个流的处理过程
    * 自定义sink
    * */
    //环境参数定义
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(100)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setRestartStrategy(RestartStrategies.fallBackRestart())

    //flink exactly_once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //定义kafka 配置,KAFKA_BROKER和消费组
    val KAFKA_BROKER = "10.28.40.11:9092,10.28.40.12:9092,10.28.40.13:9092,10.28.40.14:9092,10.28.40.15:9092"
    //val KAFKA_BROKER = "10.28.200.107:9092,10.28.200.108:9092,10.28.200.109:9092"
    val TRANSACTION_GROUP = "bbd_wanxiang_20171020"

    //初始化kafka topic
    val kafkaProps = new Properties()
    //kafkaProps.load(ClassLoader.getSystemResourceAsStream("consumer.properties"))
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    //添加source
    val streamingMessages = env.addSource(
      //wanxiang_canal_20170919
      new FlinkKafkaConsumer010[String]("wanxiang_canal_20170919", new SimpleStringSchema(), kafkaProps)).addSink(new WanxiangSinkToNeo4j())

    //streamingMessages.print()
    env.execute("Wanxiang streaming data processing")
  }

}
