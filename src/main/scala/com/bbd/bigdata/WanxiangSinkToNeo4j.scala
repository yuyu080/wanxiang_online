package com.bbd.bigdata

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

object WanxiangSinkToNeo4j {
  /*
  * 自定义flink sink ，结合我们的使用场景，实现richsinkfunction
  * 主要重写三个方法open(),invoke(),close()
  * */

  class WanxiangSinkToNeo4j extends RichSinkFunction{

    override def invoke(in: Nothing): Unit = ???
  }
}
