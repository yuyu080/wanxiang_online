package com.bbd.bigdata

import java.net.{Inet4Address, InetAddress}
import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.Config
import com.bbd.bigdata.core.{CypherToNeo4j, DeleteOperate}
import java.util.{Date, Properties}

import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import com.bbd.bigdata.redisCli.{RedisClient, RedisUtil}
import org.neo4j.driver.v1.exceptions.{ClientException, DatabaseException, TransientException}

import scala.util.Random
import com.alibaba.fastjson._
import com.bbd.bigdata.util.CommonFunctions

import scala.util.control._
import org.apache.log4j.Logger


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

    override def invoke(input: String): Unit = {
      /**
        * invoke()方法通过json信息，获取相应cypher，并插入到数据库中。
        * in 输入的数据
        * Exception processing
        */
      //打印taskmanager的ip、线程号、时间戳和接收到的数据
      val thread_id = Thread.currentThread().getId
      val addr = InetAddress.getLocalHost
      val machine_ip = addr.getHostAddress

      val check_message = CommonFunctions.analysis_input(input)
      println(machine_ip + " : " + thread_id + " : " + System.currentTimeMillis() + " : " + check_message)

      val session = driver.session()
      var table_name = ""
      val final_list = scala.collection.mutable.ListBuffer[String]()

      //所有事物执行前先根据表名进行删除操作
      val deleteOp = {
        val qyxx_id_pattern = """(?<=bbd_qyxx_id\\":\\")(.*?)(?=\\")""".r
        val qyxx_id = qyxx_id_pattern.findFirstIn(input).getOrElse("")
        if(input.contains("qyxx_baxx_canal")){
          DeleteOperate.clearBaxx(qyxx_id)
        }else if(input.contains("qyxx_gdxx_canal")){
          DeleteOperate.clearGdxx(qyxx_id)
        }else if(input.contains("qyxx_basic_canal")){
          DeleteOperate.clearBasic(qyxx_id)
        }else{
          Array("")
        }
      }

      for (item <- deleteOp) {
        if (item != "") {
          final_list += item
        }
      }

      var i = 0
      var str = "messageId_"+i
      val obj = JSON.parseObject(input)
      while(obj.get(str)!=null){
        val in = obj.get(str).toString
        i = i+1
        str = "messageId_"+i
        //一个tuple接收数据，包含（table_name,List<cypher>）
        val tmp_message = CypherToNeo4j.getCypher(in)
        table_name = tmp_message._1
        val it = tmp_message._2.iterator
        while(it.hasNext){
          final_list += it.next()
        }
      }

      var tuple_cypher_message = (table_name,final_list.toArray)
      //if(final_list.toArray.length>6) Thread.sleep(300)

      if(tuple_cypher_message._2.contains("MESSAGE_ERROR")){
        tuple_cypher_message = (table_name, Array("MESSAGE_ERROR"))
      }
      var count = 0
      val loop = new Breaks
      loop.breakable{
        while(count<4){
          count = count+1
          try{
            tuple_cypher_message._2(0) match {
              case "MESSAGE_ERROR" => put_kafka_topic(input)
              case "SINK_TO_REDIS" =>
                val len = tuple_cypher_message._2.length
                var i = 1
                while (i <= len - 1){
                  put_blacklist_redis(tuple_cypher_message._2(i))
                  i = i + 2
                }
              case _ => session.writeTransaction(new TransactionWork[Integer]() {
                override def execute(tx: Transaction): Integer = createRelation(tx,tuple_cypher_message._2)//tuple_cypher_message._2.foreach(tx.run)
              })
            }
            loop.break
          }catch {
            case e: TransientException => e.printStackTrace();put_kafka_topic(input);loop.break
            case e: ClientException => e.printStackTrace();put_kafka_topic(input);Thread.sleep(100)
            case e: DatabaseException =>e.printStackTrace();put_kafka_topic(input);loop.break
          }
        }
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
