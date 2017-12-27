package com.bbd.bigdata

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.bbd.bigdata.WanxiangSinkToNeo4j.WanxiangSinkToNeo4j
import com.bbd.bigdata.core.CypherToNeo4j
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
object WanxiangStreaming {

  def main(args: Array[String]) {
    /*
    * 程序入口，定义streaming执行环境的参数
    * 定义source，sink相关配置，以及整个流的处理过程
    * 自定义sink
    * */
    //环境参数定义
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = env.getCheckpointConfig
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/")

    //minimize latency
    env.setBufferTimeout(5)
    env.enableCheckpointing(10*60*1000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.of(10,TimeUnit.SECONDS)))
    //flink exactly_once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val params = ParameterTool.fromArgs(args)
    //定义kafka 配置,KAFKA_BROKER和消费组
    val KAFKA_BROKER = if (params.has("broker_list")) params.get("broker_list") else "c6node15:9092,c6node16:9092,c6node17:9092"
    val TRANSACTION_GROUP = if (params.has("group")) params.get("group") else "bbd_wanxiang_online_20171220"
    val topic = if(params.has("topic"))  params.get("topic") else "wanxiang_canal_20171213"
    val offset = if(params.has("offset"))  params.get("offset") else "latest"
    //初始化kafka topic
    val kafkaProps = new Properties()
    //kafkaProps.load(ClassLoader.getSystemResourceAsStream("consumer.properties"))

    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    kafkaProps.setProperty("auto.offset.reset", offset)
    //kafkaProps.setProperty("fetch.message.max.bytes", "104857600")
    kafkaProps.setProperty("max.partition.fetch.bytes", "104857600")


    //添加source
    val streamingMessages = env.addSource(
      //wanxiang_canal_20170919
      new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), kafkaProps))
      //.map(CypherToNeo4j.getCypher(_)._2).filter(_.length>1).addSink(new WanxiangSinkToNeo4j())
      //.map(CypherToNeo4j.getCypher(_)._2).filter(_.length>1).map(process_message(_)).rebalance.writeAsText("/data1/datawarehouse/data/flink_20171214").setParallelism(1)
      .uid("wanxiang_source")
      .addSink(new WanxiangSinkToNeo4j())
      .uid("wanxiang_sink")

    env.execute("Wanxiang streaming data processing to neo4j7")

  }

}


