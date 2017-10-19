package com.bbd.bigdata

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.Config
import java.util.concurrent.TimeUnit._
import com.bbd.bigdata.core.CypherToNeo4j
import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage

object WanxiangSinkToNeo4j {
  /*
  * 自定义flink sink ，结合我们的使用场景，实现richsinkfunction
  * 主要重写三个方法open(),invoke(),close()
  * */

  class WanxiangSinkToNeo4j extends RichSinkFunction[String]{
    private var driver : Driver = null

    override def open(parameters: Configuration): Unit = {
      /**
        * open方法是初始化方法，会在invoke方法之前执行，执行一次。
        */
      //neo4j 连接信息
      val conn_addr = "bolt://neo4j.dwws.bbdops.com:7690"
      val user = "neo4j"
      val passwd = "123456"
      //加载驱动
      driver = GraphDatabase.driver(conn_addr, AuthTokens.basic(user, passwd),Config.build().withMaxTransactionRetryTime( 15, SECONDS ).toConfig())
    }

    override def invoke(in: String): Unit = {
      /**
        * invoke()方法通过json信息，获取相应cypher，并插入到数据库中。
        * in 输入的数据
        * Exception processing
        */
      //一个tuple接收数据，包含（table_name,List<cypher>）

      val tuple_cypher_message = CypherToNeo4j.getCypher(in)
      println(in)
      tuple_cypher_message._2.foreach(print)
      /*tuple_cypher_message._2(0) match {
        case "message_error" => put_kafka_topic(in)
        case _ => driver.session().writeTransaction(new TransactionWork[Integer]() {
          override def execute(tx: Transaction): Integer = createRelation(tx,tuple_cypher_message._2)
        })
      }*/


    }

    def put_kafka_topic(message : String): Unit ={
      /*
      * 异常数据处理
      * 接收消息为message_error时将相应message加入到异常队列
      * 等候下一步处理
      * */
      try {
        //读取配置文件，加载配置信息
        val props = new Properties()
        props.load(ClassLoader.getSystemResourceAsStream("kafka-producer.properties"))
        val config = new ProducerConfig(props)
        val producer = new Producer[String, String](config)
        //put message to kafka topic
        val keyedMessage = new KeyedMessage[String, String]("wanxiang_exception_20171019",String.valueOf(System.currentTimeMillis()), message)
        producer.send(keyedMessage)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    def createRelation(tx: Transaction, cypher_list: Array[String]): Integer = {
      /*接收cypher list 后，解析cypher
      * 所有cypher包含在一个事务里
      * 顺序执行，异常捕获，0：异常，1：无异常
      * */
      try{
        cypher_list.foreach(tx.run)
      }catch {
        case e: Exception => e.printStackTrace();0
      }
      1
    }

    override def close() {
      if(driver != null){
        driver.close()
      }
      super.close()
    }

  }

}