package com.bbd.bigdata.util

import java.io.FileInputStream
import java.sql.Timestamp
import java.util.{Date, Properties}
import java.text.ParseException
import java.text.SimpleDateFormat

import com.alibaba.fastjson._

import scala.util.Try

/**
  * Created by Administrator on 2017/10/10.
  */
object CommonFunctions {
  lazy private val md5handle = java.security.MessageDigest.getInstance("MD5")
  private val hexDigits = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')
  private val format_one = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val format_two = new SimpleDateFormat("yyyy-MM-dd")
  private val format_three = new SimpleDateFormat("yyyy-MM")
  private val format_four = new SimpleDateFormat("yyyy")

  //获取字符串的md5值
  def md5(value: String): String = {
    val encrypt = md5handle.digest(value.getBytes)
    val b = new StringBuilder(32)
    for (i <- 0.to(15)) {
      b.append(hexDigits(encrypt(i) >>> 4 & 0xf)).append(hexDigits(encrypt(i) & 0xf))
    }
    b.mkString
  }

  //解析json
  def jsonToObj(value: String): com.alibaba.fastjson.JSONObject = {
    val ss_map  = JSON.parseObject(value)
    ss_map
  }

  //将日期统一转换成linux时间戳
  def formatDateTime(value: String): Long = {
    val result = Try {
      val time = new Timestamp(format_one.parse(value).getTime)
      time.getTime / 1000
    }.recover {
      case e: Throwable => Try {
        val time = new Timestamp(format_two.parse(value).getTime)
        time.getTime / 1000
      }.recover {
        case e: Throwable => 0L
      }.get
    }.get
    result
  }

  //将日期统一成yyyy-MM-dd格式，格式化失败则返回原始值
  def toStandardDateTime(value: String): String = {
    val result = Try {
      format_two.format(format_one.parse(value))
    }.recover {
      case e: Throwable => Try {
        format_two.format(format_two.parse(value))
      }.recover {
        case e: Throwable => ""
      }.get
    }.get
    result
  }

  //将yyyy-MM-dd格式化成yyyy-MM
  def getDateTimeMonth(value: String): String = {
    val result = Try{
      format_three.format(format_two.parse(value))
    }.recover {
      case e: Throwable => ""
    }.get
    result
  }

  //将yyyy-MM-dd格式化成yyyy
  def getDateTimeYear(value: String): String = {
    val result = Try{
      format_four.format(format_two.parse(value))
    }.recover {
      case e: Throwable => ""
    }.get
    result
  }

  //首字母大写
  def upperCase(str: String): String = {
    val ch = str.toCharArray
    if (ch(0) >= 'a' && ch(0) <= 'z') ch(0) = (ch(0) - 32).toChar
    new String(ch)
  }

  def getPersonProperty(alias_name: String): String = {
    val is_human = s"ON CREATE SET $alias_name.is_human = true"
    val dwtzxx = s"ON CREATE SET $alias_name.dwtzxx = 0"

    Array(
      is_human,
      dwtzxx
    ).reduce(
      (x, y) => x + "\n" + y
    )
  }

  def getCompanyNodePropertyName(event_table_name:String): String = {
    val event_properties = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("event_to_company_property_map.properties")
    event_properties.load(in)
    event_properties.getProperty(event_table_name)
  }

  def getCompanyProperty(alias_name: String): String = {
    //从配置文件中获取事件节点对应的企业节点属性值
    val event_to_company_property = {
      val company_properties = new Properties()
      val in = this.getClass.getClassLoader.getResourceAsStream("event_to_company_property_map.properties")
      company_properties.load(in)
      company_properties.values.toArray.filter(
        x => x != ""
      ).map(
        x => s"ON CREATE SET $alias_name.$x = 0"
      ).reduce(
        (x, y) => x+"\n"+y
      )
    }

    //角色节点对应的企业节点属性值
    val role_to_company_property = {
      val dwtzxx = s"ON CREATE SET $alias_name.dwtzxx = 0"
      val gdxx = s"ON CREATE SET $alias_name.gdxx = 0"
      val baxx = s"ON CREATE SET $alias_name.baxx = 0"
      dwtzxx + "\n" + gdxx + "\n" + baxx
    }

    //特殊类型对应的企业节点属性值
    val special_to_company_property = {
      val is_human = s"ON CREATE SET $alias_name.is_human = false"
      val isSOcompany = s"ON CREATE SET $alias_name.isSOcompany = false"
      is_human + "\n" + isSOcompany
    }

    Array(
      event_to_company_property,
      role_to_company_property,
      special_to_company_property
    ).reduce(
      (x, y) => x + "\n" + y
    )
  }


  def analysis_input(input: String): String = {
    val input_obj = jsonToObj(input)
    val json_obj = new JSONObject()
    var i = 0
    var str = "messageId_"+i

    if(input_obj.get(str)!=null){
      val in = input_obj.get(str).toString
      val message = jsonToObj(in)
      val canal_table = message.get("canal_table")
      json_obj.put("canal_table",canal_table)
    }

    json_obj.put("bbd_qyxx_id","@")
    json_obj.put("bbd_xgxx_id","@")
    json_obj.put("source_id","@")

    while(input_obj.get(str)!=null){
      val in = input_obj.get(str).toString
      i = i+1
      str = "messageId_"+i
      val message = jsonToObj(in)
      if(message.containsKey("frname_id")){
        json_obj.put("source_id",json_obj.get("source_id")+message.get("frname_id").toString+"@")
      }else if(message.containsKey("shareholder_id")){
        json_obj.put("source_id",json_obj.get("source_id")+message.get("shareholder_id").toString+"@")
      }else if(message.containsKey("name_id")){
        json_obj.put("source_id",json_obj.get("source_id")+message.get("name_id").toString+"@")
      }else if(message.containsKey("bbd_branch_id")){
        json_obj.put("source_id",json_obj.get("source_id")+message.get("bbd_branch_id").toString+"@")
      }

      if(message.containsKey("bbd_qyxx_id")){
        json_obj.put("bbd_qyxx_id",json_obj.get("bbd_qyxx_id")+message.get("bbd_qyxx_id").toString+"@")
      }

      if(message.containsKey("bd_xgxx_id")){
        json_obj.put("bbd_xgxx_id",json_obj.get("bbd_xgxx_id")+message.get("bbd_xgxx_id").toString+"@")
      }
    }

    json_obj.toString
  }

}
