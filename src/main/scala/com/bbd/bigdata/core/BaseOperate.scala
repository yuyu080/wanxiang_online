package com.bbd.bigdata.core

import java.io.FileInputStream
import java.util.Properties

import com.bbd.bigdata.util.CommonFunctions

import scala.util.Try


/**
  * Created by Administrator on 2017/10/11.
  */
object BaseOperate {

  def operateCompanyNode(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {

    //根据公司的区域编码，获取相应的省市区编码
    def getRegionInfo(company_county:String): Array[String] = Try {
      val region_properties = new Properties()
      val in = this.getClass.getClassLoader.getResourceAsStream("region_map.properties")
      region_properties.load(in)
      region_properties.getProperty(company_county).split("\\|")
    }.recover {
      case e: Throwable => Array("", "")
    }.get

    //获取地域名称
    def getRegionName(company_county:String): String = Try {
      val region_properties = new Properties()
      val in = this.getClass.getClassLoader.getResourceAsStream("region_name.properties")
      region_properties.load(in)
      new String(region_properties.getProperty(company_county).getBytes("ISO-8859-1"), "utf-8")
    }.recover {
      case e: Throwable => ""
    }.get

    /*
     *根据操作类型，返回不同的Cypther
     *企业节点无法被直接删除，DELETE返回一个查询Cypher
     */
    val table_name = info.get("canal_table").toString.replace("_canal", "")
    val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
    val event_type = info.get("canal_eventtype").toString
    val county = info.get("company_county").toString
    val county_name = getRegionName(county)
    val city = getRegionInfo(county)(0)
    val city_name = getRegionName(city)
    val province = getRegionInfo(county)(1)
    val province_name = getRegionName(province)
    val regcap_amount = info.get("regcap_amount").toString
    val realcap_amount = info.get("realcap_amount").toString

    if (event_type == "DELETE") {
      //企业节点不能被直接删除
      (
        table_name,
        Array(
          s"""
             |MATCH (b:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
             |RETURN b
         """.stripMargin
        )
      )
    } else if (event_type == "INSERT" | event_type == "UPDATE") {
      (
        table_name,
        Array(
          s"""
             |MATCH (a:Entity:Company { bbd_qyxx_id: "$bbd_qyxx_id" })-[e1:BELONG]->(b:Entity:Region)
             |SET b.company_num = b.company_num - 1
             |DELETE e1
             |WITH b
             |MATCH (b)-[e2:BELONG]->(e:Entity:Region)
             |SET e.company_num = e.company_num - 1
             |WITH e
             |MATCH (e)-[e3:BELONG]->(f:Entity:Region)
             |SET f.company_num = f.company_num - 1
         """.stripMargin,
          s"""
             |MATCH (a:Entity:Company { bbd_qyxx_id: "$bbd_qyxx_id" })-[e2:BELONG]->(b:Entity:Industry)
             |DELETE e2
             |WITH b
             |SET b.company_num = b.company_num - 1
         """.stripMargin,
          s"""
             |MERGE (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
             |${CommonFunctions.getCompanyProperty("a")}
             |ON CREATE SET a.create_time = timestamp()
             |SET a.is_ipo = ${if(info.get("ipo_company") == "") "false" else "true"}
             |SET a.name = "${info.get("company_name").toString}"
             |SET a.esdate = "${info.get("esdate").toString}"
             |SET a.address = "${info.get("address").toString.replace(",", "").replace("\"", "")}"
             |SET a.estatus = "${info.get("enterprise_status").toString.replace(",", "，")}"
             |SET a.province = "$province_name"
             |SET a.city = "$city_name"
             |SET a.county = "$county_name"
             |SET a.Industry = "${info.get("company_industry").toString}"
             |SET a.company_type = "${info.get("company_type").toString.replace(",", "").replace("\"", "")}"
             |SET a.regcap = ${if(regcap_amount == "") "0.0" else regcap_amount}
             |SET a.regcap_currency  = "${info.get("regcap_currency").toString}"
             |SET a.realcap = ${if(realcap_amount == "") "0.0" else realcap_amount}
             |SET a.realcap_currency = "${info.get("realcap_currency").toString}"
             |SET a.update_time = timestamp()
         """.stripMargin,
          s"""
             |MATCH (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" }),(b:Entity:Region {region_code : "$county" }),(e:Entity:Region {region_code : "$city" }),(f:Entity:Region {region_code : "$province" })
             |MERGE (a)-[e1:BELONG]-(b)
             |ON CREATE SET b.company_num = b.company_num + 1
             |ON CREATE SET e.company_num = e.company_num + 1
             |ON CREATE SET f.company_num = f.company_num + 1
             |ON CREATE SET e1.create_time = timestamp()
             |ON CREATE SET b.update_time = timestamp()
             |ON CREATE SET e.update_time = timestamp()
             |ON CREATE SET f.update_time = timestamp()
         """.stripMargin,
          s"""
             |MATCH (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" }),(c:Entity:Industry {industry_code: "${info.get("company_industry").toString}" })
             |MERGE (a)-[e2:BELONG]-(c)
             |ON CREATE SET c.company_num = c.company_num + 1
             |ON CREATE SET e2.create_time = timestamp()
             |ON CREATE SET c.update_time = timestamp()
         """.stripMargin

        )
      )
    } else {
      (
        table_name,
        Array("message_error")
      )
    }

  }

  //根据各个事件节点类型的不同，可能带有不同的附加属性，他们都包含在event_info里面
  def operateEventNode(info:com.alibaba.fastjson.JSONObject,
                       id:String=null,
                       event_info:String=""): Tuple2[String, Array[String]] = {

    //根据不同的表名，获取相应的事件时间字段
    def getEventColumn(table_name:String): String = Try {
      val event_time_properties = new Properties()
      val in = this.getClass.getClassLoader.getResourceAsStream("event_time_map.properties")
      event_time_properties.load(in)
      event_time_properties.getProperty(table_name)
    }.recover {
      case e: Throwable => ""
    }.get

    val table_name = info.get("canal_table").toString.replace("_canal", "")
    val event_type = info.get("canal_eventtype").toString
    val bbd_xgxx_id = if(id != null) id else info.get("bbd_xgxx_id").toString
    val event_time_column = getEventColumn(table_name)
    val event_timestamp = CommonFunctions.formatDateTime {
      val result = info.get(event_time_column)
      if (result == null) "" else result.toString
    }.toString
    val event_time = CommonFunctions.toStandardDateTime {
      val result = info.get(event_time_column)
      if (result == null) "" else result.toString
    }.toString
    val event_month = CommonFunctions.getDateTimeMonth(event_time)
    val event_year = CommonFunctions.getDateTimeYear(event_time)
    val event_label = CommonFunctions.upperCase(table_name)


    if (event_type == "DELETE") {
      (
        table_name,
        Array(
          s"""
             |MATCH (a:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })-[e1:BELONG]-(b:Entity:Time {time : "$event_time"})
             |DELETE e1
             |WITH b
             |MATCH (e:Entity:Time {time : "$event_month" }), (f:Entity:Time {time : "$event_year" })
             |SET b.company_num = b.company_num - 1
             |SET e.company_num = e.company_num - 1
             |SET f.company_num = f.company_num - 1
             |SET b.update_time = timestamp()
             |SET e.update_time = timestamp()
             |SET f.update_time = timestamp()
           """.stripMargin,
          s"""
             |MATCH (a:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })
             |DETACH DELETE a
           """.stripMargin
        )
      )

    } else if(event_type == "INSERT" | event_type == "UPDATE") {
      (
        table_name,
        Array(
          s"""
             |MATCH (a:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })-[e1:BELONG]-(b:Entity:Time)
             |SET b.company_num = b.company_num - 1
             |DELETE e1
             |WITH b
             |MATCH (b)-[e2:BELONG]->(e:Entity:Time)
             |SET e.company_num = e.company_num - 1
             |WITH e
             |MATCH (e)-[e3:BELONG]->(f:Entity:Time)
             |SET f.company_num = f.company_num - 1
         """.stripMargin,
          s"""
             |MERGE (a:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })
             |ON CREATE SET a.create_time = timestamp()
             |SET a.event_time = $event_timestamp  $event_info
             |SET a.update_time = timestamp()
         """.stripMargin,
          s"""
             |MATCH (a:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" }), (b:Entity:Time {time : "$event_time" }), (e:Entity:Time {time : "$event_month" }), (f:Entity:Time {time : "$event_year" })
             |MERGE (a)-[e1:BELONG]-(b)
             |ON CREATE SET b.company_num = b.company_num + 1
             |ON CREATE SET e.company_num = e.company_num + 1
             |ON CREATE SET f.company_num = f.company_num + 1
             |ON CREATE SET b.update_time = timestamp()
             |ON CREATE SET e.update_time = timestamp()
             |ON CREATE SET f.update_time = timestamp()
         """.stripMargin
        )
      )
    } else {
      (
        table_name,
        Array("message_error")
      )
    }
  }

  def operateEventEdge(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {

    /*
     *根据表（事件）名，获取其在企业节点中的属性名
     * 例如：qyxx_bgxx这张表对应了企业节点的bgxx属性
     */
    def getCompanyNodePropertyName(event_table_name:String): String = {
      val event_properties = new Properties()
      val in = this.getClass.getClassLoader.getResourceAsStream("event_to_company_property_map.properties")
      event_properties.load(in)
      event_properties.getProperty(event_table_name)
    }

    val table_name = info.get("canal_table").toString.replace("_canal", "")
    val event_type = info.get("canal_eventtype").toString
    val event_table_name = info.get("bbd_table").toString
    val bbd_xgxx_id = info.get("bbd_xgxx_id").toString
    val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
    val event_label = CommonFunctions.upperCase(event_table_name)
    val relation_type = event_table_name.toUpperCase
    val company_property_name = getCompanyNodePropertyName(event_table_name)

    /*
    * 1、首先判断该事件是否位于属性图中
    * 2、然后判断该事件是否是企业节点的统计属性
    * 3、根据上述问题生成不同的分支*/
    if(company_property_name != null) {
      if (event_type == "DELETE") {
        if(company_property_name != "") {
          (
            table_name,
            Array(
              s"""
                 |MATCH (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })-[e1:$relation_type]->(b:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })
                 |DELETE e1
                 |SET a.update_time = timestamp()
                 |SET a.$company_property_name = a.$company_property_name - 1
             """.stripMargin
            )
          )
        } else {
          (
            table_name,
            Array(
              s"""
                 |MATCH (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })-[e1:$relation_type]->(b:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })
                 |DELETE e1
             """.stripMargin
            )
          )
        }
      } else if(event_type == "INSERT" | event_type == "UPDATE") {
        if(company_property_name != "") {
          (
            table_name,
            Array(
              s"""
                 |MERGE (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
                 |${CommonFunctions.getCompanyProperty("a")}
                 |ON CREATE SET a.create_time = timestamp()
                 |WITH a
                 |MERGE (b:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })
                 |ON CREATE SET b.create_time = timestamp()
                 |WITH a, b
                 |MERGE (a)-[e1:$relation_type]->(b)
                 |ON CREATE SET e1.create_time = timestamp()
                 |ON CREATE SET e1.id_type = ${if(info.get("id_type") == null) 0 else info.get("id_type").toString}
                 |ON CREATE SET a.$company_property_name = a.$company_property_name + 1
                 |ON CREATE SET a.update_time = timestamp()
             """.stripMargin
            )
          )
        } else {
          (
            table_name,
            Array(
              s"""
                 |MERGE (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
                 |${CommonFunctions.getCompanyProperty("a")}
                 |ON CREATE SET a.create_time = timestamp()
                 |WITH a
                 |MERGE (b:Entity:Event:$event_label {bbd_event_id: "$bbd_xgxx_id" })
                 |ON CREATE SET b.create_time = timestamp()
                 |WITH a, b
                 |MERGE (a)-[e1:$relation_type]->(b)
                 |ON CREATE SET e1.create_time = timestamp()
                 |ON CREATE SET e1.id_type = ${info.get("id_type")}
             """.stripMargin
            )
          )
        }
      } else {
        (table_name, Array("message_error"))
      }
    } else {
      (
        table_name,
        Array(
          s"""
             |MATCH (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id"})-[e1:$relation_type]->(b:Entity:Event:Ktgg {bbd_event_id: "$bbd_xgxx_id" })
           """.stripMargin
        )
      )
    }


  }

  def operateRelationEdge(args: Map[String, String]): Tuple2[String, Array[String]] = {
    var step_one = ""
    var step_two = ""
    val step_three =
      s"""
         |MERGE (a)-[e1:IS]->(c)
         |ON CREATE SET e1.create_time = timestamp()
         |WITH a, b, c, d
         |MERGE (a)-[e2:VIRTUAL]->(d)
         |ON CREATE SET e2.create_time = timestamp()
         |WITH a, b, c, d
         |MERGE (c)-[e3:OF]->(b)
         |ON CREATE SET e3.create_time = timestamp()
         |WITH a, b, c, d
         |MERGE (d)-[e4:VIRTUAL]->(b)
         |ON CREATE SET e4.create_time = timestamp()
         |return a, b, c, d """.stripMargin

    if(args("source_label") == "Company") {
      step_one =
        s"""
           |MERGE (a:Entity:Company {bbd_qyxx_id: "${args("source_id")}" })
           |${CommonFunctions.getCompanyProperty("a")}
           |ON CREATE SET a.create_time = timestamp()
           |WITH a
           |MERGE (b:Entity:Company {bbd_qyxx_id: "${args("destination_id")}" })
           |${CommonFunctions.getCompanyProperty("b")}
           |ON CREATE SET b.create_time = timestamp()
           |WITH a, b """.stripMargin
    } else if(args("source_label") == "Person") {
      step_one =
        s"""
           |MERGE (a:Entity:Person {bbd_qyxx_id: "${args("source_id")}" })
           |ON CREATE SET a.name = "${args("source_name")}"
           |${CommonFunctions.getPersonProperty("a")}
           |ON CREATE SET a.create_time = timestamp()
           |SET a.update_time = timestamp()
           |WITH a
           |MERGE (b:Entity:Company {bbd_qyxx_id: "${args("destination_id")}" })
           |${CommonFunctions.getCompanyProperty("b")}
           |ON CREATE SET b.create_time = timestamp()
           |WITH a, b """.stripMargin
    } else {
      return (args("table_name"), Array("message_error"))
    }

    if(args("relation_type") == "INVEST") {
      step_two =
        s"""
           |MERGE (c:Entity:Role:${CommonFunctions.upperCase(args("relation_type").toLowerCase)} {bbd_role_id: "${args("bbd_role_id")}" })
           |ON CREATE SET c.create_time = timestamp()
           |SET c.update_time = timestamp()
           |SET c.role_name = "${args("role_name")}"
           |SET c.ratio = "${args("ratio")}"
           |WITH a,b,c
           |MERGE (d:Entity:Role:Isinvest {bbd_role_id: "${args("bbd_isinvest_role_id")}" })
           |ON CREATE SET d.create_time = timestamp()
           |SET d.relation_type = True
           |SET a.dwtzxx = a.dwtzxx + 1
           |SET b.gdxx = b.gdxx + 1
           |SET a.update_time = timestamp()
           |SET b.update_time = timestamp()
           |SET d.update_time = timestamp()
           |WITH a, b, c, d """.stripMargin
    } else if (args("relation_type") == "BRANCH") {
      step_two =
        s"""
           |MERGE (c:Entity:Role:${CommonFunctions.upperCase(args("relation_type").toLowerCase)} {bbd_role_id: "${args("bbd_role_id")}" })
           |ON CREATE SET c.create_time = timestamp()
           |SET c.update_time = timestamp()
           |SET c.role_name = "${args("role_name")}"
           |WITH a,b,c
           |MERGE (d:Entity:Role:Isinvest {bbd_role_id: "${args("bbd_isinvest_role_id")}" })
           |ON CREATE SET d.create_time = timestamp()
           |ON CREATE SET d.relation_type = False
           |SET b.fzjg = b.fzjg + 1
           |SET b.update_time = timestamp()
           |SET d.update_time = timestamp()
           |WITH a, b, c, d """.stripMargin
    } else {
      step_two =
        s"""
           |MERGE (c:Entity:Role:${CommonFunctions.upperCase(args("relation_type").toLowerCase)} {bbd_role_id: "${args("bbd_role_id")}" })
           |ON CREATE SET c.create_time = timestamp()
           |SET c.update_time = timestamp()
           |SET c.role_name = "${args("role_name")}"
           |WITH a,b,c
           |MERGE (d:Entity:Role:Isinvest {bbd_role_id: "${args("bbd_isinvest_role_id")}" })
           |ON CREATE SET d.create_time = timestamp()
           |ON CREATE SET d.relation_type = False
           |SET b.baxx = b.baxx + 1
           |SET b.update_time = timestamp()
           |SET d.update_time = timestamp()
           |WITH a, b, c, d """.stripMargin
    }

    if(args("event_type") == "DELETE") {
      if(args("relation_type") == "INVEST") {
        (
          args("table_name"),
          Array(
            s"""
               |MATCH
               |(a:Entity:${args("source_label")} {bbd_qyxx_id: "${args("source_id")}" }),
               |(c:Entity:Role:${CommonFunctions.upperCase(args("relation_type").toLowerCase)} {bbd_role_id: "${args("bbd_role_id")}" }),
               |(b:Entity:Company {bbd_qyxx_id: "${args("destination_id")}" })
               |DETACH DELETE c
               |SET a.dwtzxx = a.dwtzxx - 1
               |SET b.gdxx = b.gdxx - 1
               |SET b.update_time = timestamp()
               |SET a.update_time = timestamp()
               |WITH a, b
               |MATCH (a)-[:VIRTUAL]-(h:Entity:Role)-[:VIRTUAL]-(b)
               |SET h.relation_type = False
               |WITH a, b, h
               |WHERE NOT exists((a)-[:IS]-(:Entity:Role)-[:OF]-(b))
               |DETACH DELETE h
             """.stripMargin
          )
        )
      } else if (args("relation_type") == "BRANCH") {
        (
          args("table_name"),
          Array(
            s"""
               |MATCH
               |(a:Entity:Company {bbd_qyxx_id: "${args("source_id")}" }),
               |(c:Entity:Role:${CommonFunctions.upperCase(args("relation_type").toLowerCase)} {bbd_role_id: "${args("bbd_role_id")}" }),
               |(b:Entity:Company {bbd_qyxx_id: "${args("destination_id")}" })
               |DETACH DELETE c
               |SET b.fzjg = b.fzjg - 1
               |SET b.update_time = timestamp()
               |WITH a, b
               |MATCH (a)-[:VIRTUAL]-(h:Entity:Role)-[:VIRTUAL]-(b)
               |WITH a, b, h
               |WHERE NOT exists((a)-[:IS]-(:Entity:Role)-[:OF]-(b))
               |DETACH DELETE h
             """.stripMargin
          )
        )
      } else {
        (
          args("table_name"),
          Array(
            s"""
               |MATCH
               |(a:Entity:${args("source_label")} {bbd_qyxx_id: "${args("source_id")}" }),
               |(c:Entity:Role:${CommonFunctions.upperCase(args("relation_type").toLowerCase)} {bbd_role_id: "${args("bbd_role_id")}" }),
               |(b:Entity:Company {bbd_qyxx_id: "${args("destination_id")}" })
               |DETACH DELETE c
               |SET b.baxx = b.baxx - 1
               |SET b.update_time = timestamp()
               |WITH a, b
               |MATCH (a)-[:VIRTUAL]-(h:Entity:Role)-[:VIRTUAL]-(b)
               |WITH a, b, h
               |WHERE NOT exists((a)-[:IS]-(:Entity:Role)-[:OF]-(b))
               |DETACH DELETE h
             """.stripMargin
          )
        )
      }
    } else if(args("event_type") == "INSERT" | args("event_type") == "UPDATE") {
      (args("table_name"), Array(step_one + step_two + step_three))
    } else {
      (args("table_name"), Array("message_error"))
    }

  }

}
