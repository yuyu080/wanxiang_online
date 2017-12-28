package com.bbd.bigdata

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.Window
import com.bbd.bigdata.util.CommonFunctions

class MyKeyWindowFunction[K,T<: String,W <: Window] extends WindowFunction[T,T,K,W]{
  override def apply(key: K, window: W, input: Iterable[T], out: Collector[T]): Unit = {
    val it = input.iterator
    val list = scala.collection.mutable.ListBuffer[T]()
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
    for(in <- result){
      out.collect(in)
    }
  }
}
