package com.bbd.bigdata.core

import com.bbd.bigdata.util.CommonFunctions
import CompositeOperate._

/**
  * Created by Zhaoyunfeng on 2017/10/10.
  */
object CypherToNeo4j {

  /*
   *输入一个json字符串，输出一个包含多个Cypther的元组
   */
  def getCypher(arg: String): Tuple2[String, Array[String]] = {
    var table_name = ""
    try {
      val obj = CommonFunctions.jsonToObj(arg)
      var table_name = obj.get("canal_table").toString
      table_name match {
        case "qyxx_basic_canal" => qyxxBasic(obj)
        case "qyxx_state_owned_enterprise_background_canal" => qyxxStateOwnedEnterpriseBackground(obj)
        case "company_gis_canal" => companyGis(obj)
        case "xgxx_relation_canal" => xgxxRelation(obj)
        case "qyxx_gdxx_canal" => qyxxGdxx(obj)
        case "qyxx_baxx_canal" => qyxxBaxx(obj)
        case "qyxx_fzjg_merge_canal" => qyxxFzjgMerge(obj)

        case "dcos_canal" => dcos(obj)
        case "dishonesty_canal" => dishonesty(obj)
        case "ktgg_canal" => ktgg(obj)
        case "qylogo_canal" => qylogo(obj)
        case "qyxg_circxzcf_canal" => qyxgCircxzcf(obj)
        case "qyxg_jyyc_canal" => qyxgJyyc(obj)
        case "qyxg_qyqs_canal" => qyxgQyqs(obj)
        case "qyxg_yuqing_canal" => qyxgYuqing(obj)
        case "qyxx_finance_xkz_canal" => qyxxFinanceXkz(obj)
        case "qyxx_wanfang_zhuanli_canal" => qyxxWanfangZhuanli(obj)
        case "recruit_canal" => recruit(obj)
        case "rjzzq_canal" => rjzzq(obj)
        case "rmfygg_canal" => rmfygg(obj)
        case "sfpm_taobao_canal" => sfpmTaobao(obj)
        case "shgy_tdcr_canal" => shgyTdcr(obj)
        case "shgy_zhaobjg_canal" => shgyZhaobjg(obj)
        case "shgy_zhongbjg_canal" => shgyZhongbjg(obj)
        case "simutong_canal" => simutong(obj)
        case "tddkgs_canal" => tddkgs(obj)
        case "tddy_canal" => tddy(obj)
        case "tdzr_canal" => tdzr(obj)
        case "xgxx_shangbiao_canal" => xgxxShangbiao(obj)
        case "xzcf_canal" => xzcf(obj)
        case "zgcpwsw_canal" => zgcpwsw(obj)
        case "zhixing_canal" => zhixing(obj)
        case "zhuanli_zhuanyi_canal" => zhuanliZhuanyi(obj)
        case "zpzzq_canal" => zpzzq(obj)

        case "qyxx_bgxx_canal" => qyxxBgxx(obj)
        case "qyxx_liquidation_canal" => qyxxLiquidation(obj)
        case "qyxx_sharesfrost_canal" => qyxxSharesfrost(obj)
        case "qyxg_xzxk_canal" => qyxgXzxk(obj)
        case "qyxx_sharesimpawn_canal" => qyxxSharesimpawn(obj)
        case "qyxx_mordetail_canal" => qyxxMordetail(obj)
        case "domain_name_website_info_canal" => domainNameWebsiteInfo(obj)
        case "overseas_investment_canal" => overseasInvestment(obj)
        case "qyxg_debet_canal" => qyxgDebet(obj)
        case "qyxx_nb_jbxx_canal" => qyxxNbJbxx(obj)

        case "black_list_canal" => blackList(obj)
        case _ => (table_name, Array("MESSAGE_ERROR"))
      }
    } catch {
      case ex: Exception => (table_name, Array("MESSAGE_ERROR"))
    }
  }
}
