package com.bbd.bigdata

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.Window
import com.bbd.bigdata.util.CommonFunctions

class MyKeyWindowFunction[K, IN>: String, OUT>: List[IN], W <: Window] extends WindowFunction[IN,OUT,K,W]{
  override def apply(key: K, window: W, input: Iterable[IN], out: Collector[OUT]): Unit = {
    val it = input.iterator
    val list = scala.collection.mutable.ListBuffer[IN]()
    while(it.hasNext){
      list += it.next()
    }
    val result = list.toList.distinct.sortBy { x =>
      try{
        val obj = CommonFunctions.jsonToObj(x.toString)
        obj.get("canal_time").toString
      } catch {
        case e : Exception => " "
      }
    }
    out.collect(result)
  }
}
