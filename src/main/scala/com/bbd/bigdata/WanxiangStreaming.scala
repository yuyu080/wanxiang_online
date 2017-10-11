package com.bbd.bigdata

import java.util.Properties
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import org.apache.flink.streaming.api.scala._


object WanxiangStreaming {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(100)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setRestartStrategy(RestartStrategies.noRestart)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val ZOOKEEPER_HOST = "10.28.40.11:2181,10.28.40.12:2181,10.28.40.13:2181,10.28.40.14:2181,10.28.40.15:2181"   //zk的地址，下面的是kafka的地址
    val KAFKA_BROKER = "10.28.40.11:9092,10.28.40.12:9092,10.28.40.13:9092,10.28.40.14:9092,10.28.40.15:9092"
    val TRANSACTION_GROUP = "bbd_wanxiang_20171010"   //消费组

    val kafkaProps = new Properties()
    //kafkaProps.load(ClassLoader.getSystemResourceAsStream("kafka_source.properties"))
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    //kafkaProps.setProperty("auto.offset.reset","earliest")
    //env.getConfig.addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class)
    //val t  =new FlinkKafkaConsumer09[String]("test", new JsonDeserializationSchema(), kafkaProps)
    //val text = env.addSource(t)
    env.getConfig.disableSysoutLogging
    val text = env.addSource(
      new FlinkKafkaConsumer010[String](("wanxiang_canal_20170919"), new SimpleStringSchema(), kafkaProps)).map(
      x => x.trim()
      )
    text.print()

    env.execute("Wanxiang streaming data process")
  }

}
