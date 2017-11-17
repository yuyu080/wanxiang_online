package com.bbd.bigdata.core

import com.bbd.bigdata.util.CommonFunctions

/**
  * Created by Administrator on 2017/10/11.
  */
object CompositeOperate extends BaseOperate {

  def qyxxBasic(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    //操作企业节点
    val company_node_operation: Tuple2[String, Array[String]] = operateCompanyNode(info)

    //操作角色节点、关系
    val source_id = info.get("frname_id").toString
    val relation_type = "LEGAL"
    val destination_id = info.get("bbd_qyxx_id").toString
    val bbd_role_id = CommonFunctions.md5(source_id + destination_id + relation_type)
    val bbd_isinvest_role_id = CommonFunctions.md5(source_id + destination_id + "Isinvest")

    val args = Map(
      "table_name" -> info.get("canal_table").toString.replace("_canal", ""),
      "event_type" -> info.get("canal_eventtype").toString,
      "source_label" -> {
        val name_compid = info.get("frname_compid").toString
        if(name_compid == "0") "Company" else if(name_compid == "1") "Person" else ""
      },
      "source_id" -> source_id,
      "source_name" -> info.get("frname").toString,
      "relation_type" -> relation_type,
      "destination_id" -> destination_id,
      "bbd_role_id" -> bbd_role_id,
      "role_name" -> "法定代表人",
      "bbd_isinvest_role_id" -> bbd_isinvest_role_id
    )

    val old_relation_edge_operation = operateRelationEdge(args)

    (company_node_operation._1, company_node_operation._2 ++ old_relation_edge_operation._2)

  }

  def xgxxRelation(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventEdge(info)
  }

  def qyxxGdxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {

    val source_id = info.get("shareholder_id").toString
    val relation_type = "INVEST"
    val destination_id = info.get("bbd_qyxx_id").toString
    val bbd_role_id = CommonFunctions.md5(source_id + destination_id + relation_type)
    val bbd_isinvest_role_id = CommonFunctions.md5(source_id + destination_id + "Isinvest")

    val args = Map(
      "table_name" -> info.get("canal_table").toString.replace("_canal", ""),
      "event_type" -> info.get("canal_eventtype").toString,
      "source_label" -> {
        val name_compid = info.get("name_compid").toString
        if(name_compid == "0") "Company" else if(name_compid == "1") "Person" else ""
      },
      "source_id" -> source_id,
      "source_name" -> info.get("shareholder_name").toString,
      "relation_type" -> relation_type,
      "destination_id" -> destination_id,
      "bbd_role_id" -> bbd_role_id,
      "role_name" -> info.get("shareholder_type").toString,
      "bbd_isinvest_role_id" -> bbd_isinvest_role_id,
      "ratio" -> info.get("invest_ratio").toString
    )

    operateRelationEdge(args)
  }

  def qyxxBaxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val source_id = info.get("name_id").toString
    val relation_type = info.get("type").toString.toUpperCase
    val destination_id = info.get("bbd_qyxx_id").toString
    val bbd_role_id = CommonFunctions.md5(source_id + destination_id + relation_type)
    val bbd_isinvest_role_id = CommonFunctions.md5(source_id + destination_id + "Isinvest")

    val args  = Map(
      "table_name" -> info.get("canal_table").toString.replace("_canal", ""),
      "event_type" -> info.get("canal_eventtype").toString,
      "source_label" -> "Person",
      "source_id" -> source_id,
      "source_name" -> info.get("name").toString,
      "relation_type" -> relation_type,
      "destination_id" -> destination_id,
      "bbd_role_id" -> bbd_role_id,
      "role_name" -> info.get("position").toString,
      "bbd_isinvest_role_id" -> bbd_isinvest_role_id
    )

    operateRelationEdge(args)
  }

  def qyxxFzjgMerge(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val source_id = info.get("bbd_branch_id").toString
    val relation_type = "BRANCH"
    val destination_id = info.get("bbd_qyxx_id").toString
    val bbd_role_id = CommonFunctions.md5(source_id + destination_id + relation_type)
    val bbd_isinvest_role_id = CommonFunctions.md5(source_id + destination_id + "Isinvest")

    val args = Map(
      "table_name" -> info.get("canal_table").toString.replace("_canal", ""),
      "event_type" -> info.get("canal_eventtype").toString,
      "source_label" -> "Company",
      "source_id" -> source_id,
      "source_name" -> info.get("name").toString,
      "relation_type" -> relation_type,
      "destination_id" -> destination_id,
      "bbd_role_id" -> bbd_role_id,
      "role_name" -> "分支机构",
      "bbd_isinvest_role_id" -> bbd_isinvest_role_id
    )

    operateRelationEdge(args)
  }

  def qyxxStateOwnedEnterpriseBackground(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val table_name = info.get("canal_table").toString.replace("_canal", "")
    val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
    val event_type = info.get("canal_eventtype").toString

    if (event_type == "INSERT" | event_type == "UPDATE") {
      (
        table_name,
        Array(
          s"""
             |MERGE (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
             |ON CREATE SET a.ktgg = 0
             |ON CREATE SET a.zgcpwsw = 0
             |ON CREATE SET a.rmfygg = 0
             |ON CREATE SET a.xzcf = 0
             |ON CREATE SET a.zhixing = 0
             |ON CREATE SET a.dishonesty = 0
             |ON CREATE SET a.shangbiao = 0
             |ON CREATE SET a.zhongbiao = 0
             |ON CREATE SET a.zhaobiao = 0
             |ON CREATE SET a.zhuanli = 0
             |ON CREATE SET a.taxs = 0
             |ON CREATE SET a.bgxx = 0
             |ON CREATE SET a.recruit = 0
             |ON CREATE SET a.fzjg = 0
             |ON CREATE SET a.jyyc = 0
             |ON CREATE SET a.black = 0
             |ON CREATE SET a.create_time = timestamp()
             |SET a.isSOcompany = True
           """.stripMargin
        )
      )
    } else if (event_type == "DELETE") {
      (
        table_name,
        Array(
          s"""
             |MERGE (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
             |${CommonFunctions.getCompanyProperty("a")}
             |ON CREATE SET a.create_time = timestamp()
             |SET a.isSOcompany = False
           """.stripMargin
        )
      )
    } else {
      (table_name, Array("MESSAGE_ERROR"))
    }
  }

  def companyGis(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val table_name = info.get("canal_table").toString.replace("_canal", "")
    val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
    val event_type = info.get("canal_eventtype").toString
    val gis_lon = info.get("company_gis_lon")
    val gis_lat = info.get("company_gis_lat")

    if (event_type == "INSERT" | event_type == "UPDATE") {
      (
        table_name,
        Array(
          s"""
             |MERGE (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
             |${CommonFunctions.getCompanyProperty("a")}
             |ON CREATE SET a.create_time = timestamp()
             |SET a.gis_lon = ${if(gis_lon != "null") gis_lon.toString else "0"}
             |SET a.gis_lat = ${if(gis_lat != "null") gis_lat.toString else "0"}
           """.stripMargin
        )
      )
    } else if (event_type == "DELETE") {
      (
        table_name,
        Array(
          s"""
             |MERGE (a:Entity:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
             |RETURN a
           """.stripMargin
        )
      )
    } else {
      (table_name, Array("MESSAGE_ERROR"))
    }
  }


  def dcos(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def dishonesty(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def ktgg(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }


  def qylogo(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxgCircxzcf(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxgJyyc(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxgQyqs(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxgYuqing(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxgYuqingMain(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxFinanceXkz(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxWanfangZhuanli(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }


  def recruit(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def rjzzq(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def rmfygg(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def sfpmTaobao(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def shgyTdcr(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def shgyZhaobjg(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def shgyZhongbjg(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def simutong(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def tddkgs(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def tddy(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def tdzr(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def xgxxShangbiao(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def xzcf(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def zgcpwsw(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def zhixing(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def zhuanliZhuanyi(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def zpzzq(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxBgxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    //根据特殊规则，生成ID
    val bbd_xgxx_id = CommonFunctions.md5 {
      val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
      val company_name = info.get("company_name").toString
      val change_date = info.get("change_date").toString
      val change_items = info.get("change_items").toString
      val content_before_change = info.get("content_before_change").toString
      val content_after_change = info.get("content_after_change").toString
      bbd_qyxx_id + company_name + change_date + change_items + content_before_change + content_after_change
    }
    val event_node_operation = operateEventNode(info, id=bbd_xgxx_id)

    info.put("bbd_table", "qyxx_bgxx")
    info.put("bbd_xgxx_id", bbd_xgxx_id)
    val event_edge_operation = operateEventEdge(info)

    (event_node_operation._1, event_edge_operation._2 ++ event_node_operation._2)
  }

  def qyxxLiquidation(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val bbd_xgxx_id = CommonFunctions.md5 {
      val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
      val company_name = info.get("company_name").toString
      val ligentity = info.get("ligentity").toString
      val ligprincipal = info.get("ligprincipal").toString
      val liqmen = info.get("liqmen").toString
      val ligst = info.get("ligst").toString
      val ligenddate = info.get("ligenddate").toString
      val debttranee = info.get("debttranee").toString
      val claimtranee = info.get("claimtranee").toString
      bbd_qyxx_id + company_name + ligentity + ligprincipal + liqmen + ligst + ligenddate + debttranee + claimtranee
    }
    val event_node_operation = operateEventNode(info, id=bbd_xgxx_id)

    info.put("bbd_table", "qyxx_liquidation")
    info.put("bbd_xgxx_id", bbd_xgxx_id)
    val event_edge_operation = operateEventEdge(info)

    (event_node_operation._1, event_edge_operation._2 ++ event_node_operation._2)
  }

  def qyxxSharesfrost(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val bbd_xgxx_id = CommonFunctions.md5 {
      val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
      val company_name = info.get("company_name").toString
      val frodocno = info.get("frodocno").toString
      val froauth = info.get("froauth").toString
      val frofrom = info.get("frofrom").toString
      val froto = info.get("froto").toString
      val froam = info.get("froam").toString
      val thawauth = info.get("thawauth").toString
      val thawdocno = info.get("thawdocno").toString
      val thawdate = info.get("thawdate").toString
      bbd_qyxx_id  + company_name + frodocno + froauth + frofrom + froto + froam + thawauth + thawdocno + thawdate
    }
    val event_node_operation = operateEventNode(info, id=bbd_xgxx_id)

    info.put("bbd_table", "qyxx_sharesfrost")
    info.put("bbd_xgxx_id", bbd_xgxx_id)
    val event_edge_operation = operateEventEdge(info)

    (event_node_operation._1, event_edge_operation._2 ++ event_node_operation._2)
  }

  def qyxgXzxk(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxSharesimpawn(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val bbd_xgxx_id = CommonFunctions.md5 {
      val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
      val company_name = info.get("company_name").toString
      val imporg = info.get("imporg").toString
      val imporgtype = info.get("imporgtype").toString
      val impam = info.get("impam").toString
      val imponrecdate = info.get("imponrecdate").toString
      val impexaeep = info.get("impexaeep").toString
      val impsandate = info.get("impsandate").toString
      val impto = info.get("impto").toString
      val morregcno = info.get("morregcno").toString
      val imporg_idno = info.get("imporg_idno").toString
      val pledgee = info.get("pledgee").toString
      val pledgee_idno = info.get("pledgee_idno").toString
      val impstate = info.get("impstate").toString
      val impsituation = info.get("impsituation").toString
      bbd_qyxx_id  + company_name + imporg + imporgtype + impam + imponrecdate + impexaeep + impsandate + impto + morregcno + imporg_idno + pledgee + pledgee_idno + impstate + impsituation
    }
    val event_node_operation = operateEventNode(info, id=bbd_xgxx_id)

    info.put("bbd_table", "qyxx_sharesimpawn")
    info.put("bbd_xgxx_id", bbd_xgxx_id)
    val event_edge_operation = operateEventEdge(info)

    (event_node_operation._1, event_edge_operation._2 ++ event_node_operation._2)
  }

  def qyxxMordetail(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    val bbd_xgxx_id = CommonFunctions.md5 {
      val bbd_qyxx_id = info.get("bbd_qyxx_id").toString
      val company_name = info.get("company_name").toString
      val morreg_id = info.get("morreg_id").toString
      val mortgagor = info.get("mortgagor").toString
      val more = info.get("more").toString
      val regorg = info.get("regorg").toString
      val regidate = info.get("regidate").toString
      val mortype = info.get("mortype").toString
      val morregcno = info.get("morregcno").toString
      val appregrea = info.get("appregrea").toString
      val priclaseckind = info.get("priclaseckind").toString
      val priclasecam = info.get("priclasecam").toString
      val pefperform = info.get("pefperform").toString
      val pefperto = info.get("pefperto").toString
      val candate = info.get("candate").toString
      val guaname = info.get("guaname").toString
      val guadetali = info.get("guadetali").toString
      bbd_qyxx_id + company_name + morreg_id + mortgagor + more + regorg + regidate + mortype + morregcno + appregrea + priclaseckind + priclasecam + pefperform + pefperto + candate + guaname + guadetali
    }
    val event_node_operation = operateEventNode(info, id=bbd_xgxx_id)

    info.put("bbd_table", "qyxx_mordetail")
    info.put("bbd_xgxx_id", bbd_xgxx_id)
    val event_edge_operation = operateEventEdge(info)

    (event_node_operation._1, event_edge_operation._2 ++ event_node_operation._2)
  }

  def domainNameWebsiteInfo(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def overseasInvestment(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbJbxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbGzsm(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbCzxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbWzxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbFzjg(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbTzxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbZcxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbDbxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbXgxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbXzxk(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

  def qyxxNbBgxx(info:com.alibaba.fastjson.JSONObject): Tuple2[String, Array[String]] = {
    operateEventNode(info)
  }

}
