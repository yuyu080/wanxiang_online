package com.bbd.bigdata

import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.Config
import com.bbd.bigdata.core.CypherToNeo4j
import java.util.{Date, Properties}

import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import com.bbd.bigdata.redisCli.{RedisClient, RedisUtil}
import org.neo4j.driver.v1.exceptions.{ClientException, DatabaseException, TransientException}

import scala.util.Random


object WanxiangSinkToNeo4j {

  /*
  * 自定义flink sink ，结合我们的使用场景，实现richsinkfunction
  * 主要重写三个方法open(),invoke(),close()
  * */
  class WanxiangSinkToNeo4j extends RichSinkFunction[String] {
    private var driver: Driver = null
    //private var jr: redis.clients.jedis.Jedis =null

    override def open(parameters: Configuration): Unit = {
      /**
        * open方法是初始化方法，会在invoke方法之前执行，执行一次。
        */
      //neo4j 连接信息
      //测试neo4j bolt://10.28.102.33:7687  正式 bolt://10.28.52.151:7690 neo4j fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfB
      //正式 bolt://10.28.62.48:7687 wanxiangstream 2a3b73d7145adbf899536702ecc71855
      val conn_addr = "bolt://10.28.62.48:7687"
      val user = "wanxiangstream"
      val passwd = "2a3b73d7145adbf899536702ecc71855"
      //加载驱动

      driver = GraphDatabase.driver(conn_addr, AuthTokens.basic(user, passwd), Config.build().withMaxSessions(1).withMaxTransactionRetryTime( 15,TimeUnit.SECONDS ).toConfig())

    }

    override def invoke(in: String): Unit = {
      /**
        * invoke()方法通过json信息，获取相应cypher，并插入到数据库中。
        * in 输入的数据
        * Exception processing
        */
      //一个tuple接收数据，包含（table_name,List<cypher>）

      val session = driver.session()
      val tuple_cypher_message = CypherToNeo4j.getCypher(in)

      try{
        tuple_cypher_message._2(0) match {
          case "MESSAGE_ERROR" => put_kafka_topic(in)
          case "SINK_TO_REDIS" => put_blacklist_redis(tuple_cypher_message._2(1))
          case _ => session.writeTransaction(new TransactionWork[Integer]() {
            override def execute(tx: Transaction): Integer = createRelation(tx,tuple_cypher_message._2)//tuple_cypher_message._2.foreach(tx.run)
          })
        }
      }catch {
        case e: TransientException => e.printStackTrace();message_process_retry(in,session,tuple_cypher_message._2,1)
        case e: ClientException => e.printStackTrace();message_process_retry(in,session,tuple_cypher_message._2,3)
        case e: DatabaseException =>e.printStackTrace();put_kafka_topic(in+e.toString)
      }
      session.close()
    }

    def message_process_retry(in: String,session: Session, cypher_list: Array[String], retry_times: Int): Unit = {
      if(retry_times > 0){
        try{
          session.writeTransaction(new TransactionWork[Integer]() {
            override def execute(tx: Transaction): Integer = createRelation(tx,cypher_list)//tuple_cypher_message._2.foreach(tx.run)
            })
        }catch {
          case e: ClientException => message_process_retry(in,session,cypher_list,retry_times-1)

        }
      }else{
        put_kafka_topic(in)
      }

    }

    def put_blacklist_redis(bbd_qyxx_id: String): Unit = {
      /*
        * 黑名单处理
        * 接收blcak_list表中qyxx_id
        * 将其写入redis 一个value的set
        * */
      try {
        val jedis = RedisClient.pool.getResource
        jedis.sadd("wx_graph_black_set", bbd_qyxx_id)
        RedisClient.pool.returnResource(jedis)
      } catch {
        case e: Exception =>
          println("redis error! "+new Date()); e.printStackTrace()
      }
    }

    def put_kafka_topic(message: String): Unit = {
      /*
      * 异常数据处理
      * 接收消息为message_error时将相应message加入到异常队列
      * 等候下一步处理
      * */
      try {
        //读取配置文件，加载配置信息
        val props = new Properties()
        props.put("metadata.broker.list", "c6node15:9092,c6node16:9092,c6node17:9092")
        props.put("request.required.acks", "1")
        //props.put("message.send.max.retries", "20")
        props.put("serializer.class", "kafka.serializer.StringEncoder")
        val config = new ProducerConfig(props)
        val producer = new Producer[String, String](config)
        //put message to kafka topic
        val keyedMessage = new KeyedMessage[String, String]("wanxiang_exception_20171019", String.valueOf(System.currentTimeMillis()), message)
        producer.send(keyedMessage)
      } catch {
        case e: Exception => println("kafka error! "+ new Date()); e.printStackTrace()
      }
    }

    def createRelation(tx: Transaction, cypher_list: Array[String]): Integer = {
      /*接收cypher list 后，解析cypher
      * 所有cypher包含在一个事务里
      * 顺序执行，异常捕获，0：异常，1：无异常
      * */
      cypher_list.foreach(tx.run)
      1
    }

    override def close() {
      if (driver != null) {
        driver.close()
      }

      super.close()
    }


  }

}
