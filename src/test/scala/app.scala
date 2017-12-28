/**
  * Created by Administrator on 2017/9/10.
  */

import java.util.concurrent.TimeUnit._

import com.bbd.bigdata.core.CypherToNeo4j
import org.neo4j.driver.v1.{Config, _}


object app {
  def main(args: Array[String]) {
    val driver = GraphDatabase.driver("bolt://10.28.102.32:7687", AuthTokens.basic("neo4j", "fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfB"),
      Config.build().withConnectionTimeout(3, SECONDS).toConfig()
    )

    def addPerson(cyphers: Tuple2[String, Array[String]]) = {
      try {
        val session = driver.session()
        session.writeTransaction(new TransactionWork[Integer]() {
          override def execute(tx: Transaction) = createRelation(tx, cyphers)
        })
        session.close()
      } catch {
        case es: Exception => println(es.getMessage)
      }
    }

    def createRelation(tx: Transaction, cyphers: Tuple2[String, Array[String]]): Int = {
      for (each_cyoher <- cyphers._2) {
        tx.run(each_cyoher)
      }
      tx.success()
      1
    }


    //    println(CommonFunctions.md5("zhaoyunfeng"))
    val json =
      """{"esdate": "2017-06-30","openfrom":"","company_type":"个体工商户","operating_period":"","opento":"","regcap_amount":"","type":"浙江","company_province":"浙江","bbd_qyxx_id":"2288aafc5f9d4d058d3c5eb19f4c220a","credit_code":"92330203MA2927PF0H","cancel_date":"","regorg":"宁波市海曙区场监督管理局","id":"","investcap_currency":"","realcap":"","bbd_type":"zhejiang","canal_time":"2017/09/26 10:51:07","company_companytype":"9300","regno":"","approval_date":"2017-06-30","create_time":"2017-09-22 15:29:41","frname_id":"4821cc091d267e367be424b514ab98db","regcapcur":"","bbd_history_name":"[]","company_name":"宁波市海曙古林酒肆烧烤店","ipo_company":"","canal_eventtype":"INSERT","operate_scope":"餐饮服务。（法须经批准的项目，经相关部门批准后方可开展经营活动）","revoke_date":"","realcap_amount":"3443.4","frname_compid":"0","canal_table":"qyxx_basic_canal","regno_or_creditcode":"92330203MA2927PF0H","parent_firm":"","company_enterprise_status":"存续","company_currency":"","bbd_uptime":"1506065217","frname":"牛威龙","address":"浙江省宁波市海曙区古林镇俞家村南街315号","investcap_amount":"","regcap_currency":"","enterprise_status":"存续","company_county":"530324","company_industry":"H","form":"个人经营","regcap":"","company_regorg":"330201","invest_cap":"","bbd_dotime":"2017-09-22","realcap_currency":""}"""
    val dejson =
      """{"esdate": "2017-06-30","openfrom":"","company_type":"个体工商户","operating_period":"","opento":"","regcap_amount":"","type":"浙江","company_province":"浙江","bbd_qyxx_id":"2288aafc5f9d4d058d3c5eb19f4c220a","credit_code":"92330203MA2927PF0H","cancel_date":"","regorg":"宁波市海曙区场监督管理局","id":"","investcap_currency":"","realcap":"","bbd_type":"zhejiang","canal_time":"2017/09/26 10:51:07","company_companytype":"9300","regno":"","approval_date":"2017-06-30","create_time":"2017-09-22 15:29:41","frname_id":"4821cc091d267e367be424b514ab98db","regcapcur":"","bbd_history_name":"[]","company_name":"宁波市海曙古林酒肆烧烤店","ipo_company":"","canal_eventtype":"DELETE","operate_scope":"餐饮服务。（法须经批准的项目，经相关部门批准后方可开展经营活动）","revoke_date":"","realcap_amount":"3443.4","frname_compid":"0","canal_table":"qyxx_basic_canal","regno_or_creditcode":"92330203MA2927PF0H","parent_firm":"","company_enterprise_status":"存续","company_currency":"","bbd_uptime":"1506065217","frname":"牛威龙","address":"浙江省宁波市海曙区古林镇俞家村南街315号","investcap_amount":"","regcap_currency":"","enterprise_status":"存续","company_county":"530324","company_industry":"H","form":"个人经营","regcap":"","company_regorg":"330201","invest_cap":"","bbd_dotime":"2017-09-22","realcap_currency":""}"""


    val json2 = """{"esdate":"2017-09-15","openfrom":"2017-09-15","company_type":"有限责任公司（自然人投资或控股）","operating_period":"","opento":"2027-09-14","regcap_amount":"2000000.0","type":"上海","company_province":"上海","bbd_qyxx_id":"417edb413bfe44bbbcb477a3bfc03d54","credit_code":"91310116MA1J9WY5X8","cancel_date":"","regorg":"金山区市场监管局","id":"","investcap_currency":"","realcap":"","bbd_type":"shanghai","canal_time":"2017/10/11 13:50:53","company_companytype":"1130","regno":"","approval_date":"2017-09-15","create_time":"2017-10-10 19:16:23","frname_id":"dd15f8b1887f000f079155e16b037561","regcapcur":"","bbd_history_name":"[]","company_name":"上海璨晔文化传播有限公司","ipo_company":"","canal_eventtype":"INSERT","operate_scope":"文化艺术交流策划咨询，商务咨询，企业管理咨询，公关活动策划，计算机网络工程，办公文化用品，电子产品，计算机、软件及辅助设备销售，动漫设计，从事网络科技专业领域内技术开发技术转让、技术咨询、技术服务。【依法须经批准的项目，经相关部门批准后方可开展经营活动】","revoke_date":"","realcap_amount":"","frname_compid":"1","canal_table":"qyxx_basic_canal","regno_or_creditcode":"","parent_firm":"","company_enterprise_status":"存续","company_currency":"人民币","bbd_uptime":"1507628644","frname":"武晔","address":"上海市金山区金山卫镇秋实路688号1号楼5单元304室D座","investcap_amount":"","regcap_currency":"人民币","enterprise_status":"存续（在营,开业、在册）","company_county":"310116","company_industry":"R","form":"","regcap":"200万人民币","company_regorg":"310012","invest_cap":"","bbd_dotime":"2017-10-10","realcap_currency":""}"""
    val json3 = """{"comment_num":"","news_site":"中国财经信息网","canal_table":"qyxg_yuqing_canal","main":"","plate":"","table_name":"","bbd_source":"百度新闻","rowkey":"","id":"50880614","keyword":"","bbd_type":"qyxg_yuqing_baidu_news","canal_time":"2017/10/11 14:26:13","bbd_uptime":"1507543283","bbd_url":"http://www.cfi.net.cn/p20160326000415.html","pubdate":"2017-07-21","total_news":"8","ipgp":"","search_key":"贵州燃气集团股份有限公司","create_time":"2017-10-09 18:03:41","bbd_xgxx_id":"c4b349d9f365d7c423dc8ed196072aca","author":"","abstract":"及相关法律、法规、规范性文件编写本报书。 二、本信息披露义务人签署本报告书已获得必要的授权和批准,其履行亦不 违反信息披露义务人章程或内部规则中的任何条款...","click_num":"","picture":"","news_num":"0","transfer_num":"","canal_eventtype":"INSERT","news_title":"...报告书(刘江、和泓置地集团有限公司、贵州燃气集团股份有限公司)","_id":"","bbd_dotime":"2017-10-09","status":""}"""


    val json4 = """{"bbd_table":"recruit","create_time":"2017-09-22 14:44:56","bbd_qyxx_id":"190190ec398c4524915ff2378d692ea4","canal_table":"xgxx_relation_canal","bbd_xgxx_id":"412389d6cc9cd963e7d8cd2df4490222","canal_eventtype":"INSERT","id_type":"0","id":"1153282193","canal_time":"2017/09/26 10:50:51"}"""
    val json4ed = """{"bbd_table":"recruit","create_time":"2017-09-22 14:44:56","bbd_qyxx_id":"190190ec398c4524915ff2378d692ea4","canal_table":"xgxx_relation_canal","bbd_xgxx_id":"412389d6cc9cd963e7d8cd2df4490222","canal_eventtype":"DELETE","id_type":"0","id":"1153282193","canal_time":"2017/09/26 10:50:51"}"""
    val json44 = """{"bbd_table":"recruit","create_time":"2017-09-22 14:44:56","bbd_qyxx_id":"190190ec398c4524915ff2378d692ea4","canal_table":"xgxx_relation_canal","bbd_xgxx_id":"zyf412389d6cc9cd963e7d8cd2df4490222","canal_eventtype":"INSERT","id_type":"0","id":"1153282193","canal_time":"2017/09/26 10:50:51"}"""
    val json44ed = """{"bbd_table":"recruit","create_time":"2017-09-22 14:44:56","bbd_qyxx_id":"190190ec398c4524915ff2378d692ea4","canal_table":"xgxx_relation_canal","bbd_xgxx_id":"zyf412389d6cc9cd963e7d8cd2df4490222","canal_eventtype":"DELETE","id_type":"0","id":"1153282193","canal_time":"2017/09/26 10:50:51"}"""



    val json5de = """{"no":"2","create_time":"2017-06-21 12:24:47","sumconam":"","invest_amount":"70.0","canal_table":"qyxx_gdxx_canal","invest_ratio":"70.0","shareholder_type":"企业法人","subscribed_capital":"70.0","idno":"360421210000467","idtype":"","bbd_qyxx_id":"03e1236becad4f848fddc112af408a0c","shareholder_detail":"","company_name":"九江高旺贸易有限公司","paid_contribution":"2.1","shareholder_id":"cf20d42646814390adda48367d71bbc6","canal_eventtype":"DELETE","id":"419433305","shareholder_name":"华林特钢集团有限公司","canal_time":"2017/10/13 00:00:02","invest_name":"!","name_compid":"0","bbd_dotime":"2017-06-19","bbd_uptime":"1497879472"}"""
    val json5 = """{"no":"2","create_time":"2017-06-21 12:24:47","sumconam":"","invest_amount":"70.0","canal_table":"qyxx_gdxx_canal","invest_ratio":"70.0","shareholder_type":"企业法人","subscribed_capital":"70.0","idno":"360421210000467","idtype":"","bbd_qyxx_id":"03e1236becad4f848fddc112af408a0c","shareholder_detail":"","company_name":"九江高旺贸易有限公司","paid_contribution":"2.1","shareholder_id":"cf20d42646814390adda48367d71bbc6","canal_eventtype":"INSERT","id":"419433305","shareholder_name":"华林特钢集团有限公司","canal_time":"2017/10/13 00:00:02","invest_name":"!","name_compid":"0","bbd_dotime":"2017-06-19","bbd_uptime":"1497879472"}"""
    val json55de = """{"no":"2","create_time":"2017-06-21 12:24:47","sumconam":"","invest_amount":"70.0","canal_table":"qyxx_gdxx_canal","invest_ratio":"70.0","shareholder_type":"企业法人","subscribed_capital":"70.0","idno":"360421210000467","idtype":"","bbd_qyxx_id":"03e1236becad4f848fddc112af408a0c","shareholder_detail":"","company_name":"zyf九江高旺贸易有限公司","paid_contribution":"2.1","shareholder_id":"zyfcf20d42646814390adda48367d71bbc6","canal_eventtype":"DELETE","id":"419433305","shareholder_name":"zyf华林特钢集团有限公司","canal_time":"2017/10/13 00:00:02","invest_name":"!","name_compid":"0","bbd_dotime":"2017-06-19","bbd_uptime":"1497879472"}"""
    val json55 = """{"no":"2","create_time":"2017-06-21 12:24:47","sumconam":"","invest_amount":"70.0","canal_table":"qyxx_gdxx_canal","invest_ratio":"70.0","shareholder_type":"企业法人","subscribed_capital":"70.0","idno":"360421210000467","idtype":"","bbd_qyxx_id":"03e1236becad4f848fddc112af408a0c","shareholder_detail":"","company_name":"zyf九江高旺贸易有限公司","paid_contribution":"2.1","shareholder_id":"zyfcf20d42646814390adda48367d71bbc6","canal_eventtype":"INSERT","id":"419433305","shareholder_name":"zyf华林特钢集团有限公司","canal_time":"2017/10/13 00:00:02","invest_name":"!","name_compid":"0","bbd_dotime":"2017-06-19","bbd_uptime":"1497879472"}"""


    val json6 = """{"resume":"","no":"","canal_table":"qyxx_baxx_canal","sex":"","type":"director","salary":"","idno":"","name_id":"6eec36483c56ce85e85bd5fedfa98471","idtype":"","bbd_qyxx_id":"9f74c3bb7e544c3a8b439856704e71d6","company_name":"北斗旭普空间信息产业（武汉）有限公司","name":"冉崇国","canal_eventtype":"INSERT","id":"545302591","position":"执行董事","asstarting":"","canal_time":"2017/10/16 00:00:01","bbd_dotime":"2017-07-29","bbd_uptime":"1501338652"}"""
    val json7 = """{"canal_table":"qyxx_bgxx_canal","change_date":"2017-07-14","change_count":"","content_after_change":"许可经营项目：无 一般经营项目：机设备租赁；标牌、标线、石料、沥青销售；沥青商砼、改性沥青、乳化沥青加工销售；道路普通货物运输（凭相关许可经营）。（依法须经批准的项目，经相关部门批准后方可开展经营活动。）","change_items":"经营范围","bbd_qyxx_id":"bda599462d0345f5ac81287b3383bf70","content_before_change":"许可经营项目：无 一般经营项目：机械设备租赁；标牌、标线、石料、沥青销售；沥青商砼、改性沥青、乳化沥青加工销售（凭相关许可经营）。（依法经批准的项目，经相关部门批准后方可开展经营活动。）","company_name":"阿拉善盟同进工贸有限责任公司","canal_eventtype":"INSERT","id":"359117910","canal_time":"2017/10/17 18:52:25","bbd_dotime":"2017-10-17","bbd_uptime":"1508237376"}"""
    val json8 = """{"regno":"610132200015124","esdate":"2014-12-02","no":"None","address":"西安经济技术开发区凤城二路海璟国际第7幢1层10109号房","create_time":"2017-09-01 20:57:09","canal_table":"qyxx_fzjg_merge_canal","enterprise_status":"存续","regno_or_creditcode":"91610132311054225B","bbd_branch_id":"c64a59684ba4421486314bf318b28aac","bbd_qyxx_id":"c3970f5cd81544e8873ee14c692d52c2","company_name":"陕西火麒麟财务咨询有限公司","name":"陕西火麒麟财务咨询有限公司西安第一分公司","regorg":"西安市商行政管理局经开分局","canal_eventtype":"DELETE","id":"4681982","canal_time":"2017/10/19 00:11:21","frname":"姚洋","bbd_dotime":"2017-10-18","bbd_uptime":"1508316806"}"""

    val json9 = """{"resume":"","no":"","canal_table":"qyxx_baxx_canal","sex":"","type":"executive","salary":"","idno":"","name_id":"ff1c25a152f03192906fff095e14bb15","idtype":"","bbd_qyxx_id":"191c54047e384721b018e9a50ed7169b","company_name":"宁安市泽林粮食经销有限公司","name":"杨金才","canal_eventtype":"INSERT","id":"595495290","position":"总经理","asstarting":"","canal_time":"2017/10/19 12:21:19","bbd_dotime":"2017-10-18","bbd_uptime":"1508288853"}"""
    val json9ed = """{"resume":"","no":"","canal_table":"qyxx_baxx_canal","sex":"","type":"executive","salary":"","idno":"","name_id":"ff1c25a152f03192906fff095e14bb15","idtype":"","bbd_qyxx_id":"191c54047e384721b018e9a50ed7169b","company_name":"宁安市泽林粮食经销有限公司","name":"杨金才","canal_eventtype":"DELETE","id":"595495290","position":"总经理","asstarting":"","canal_time":"2017/10/19 12:21:19","bbd_dotime":"2017-10-18","bbd_uptime":"1508288853"}"""


    val json10 = """{"canal_table":"qyxx_bgxx_canal","change_date":"2017-03-03","change_count":"","content_after_change":"王萍","change_items":"负责人变更（法定代表人、负责人、首席代表、合伙事务执行人等变更）变更","bbd_qyxx_id":"27b9479f6a304facbfb646fa9935e9a5","content_before_change":"雷运强","company_name":"石首市祥运船务有限公司","canal_eventtype":"INSERT","id":"293480397","canal_time":"2017/10/23 23:59:58","bbd_dotime":"2017-07-09","bbd_uptime":"1499599370"}"""
    val json10ed = """{"canal_table":"qyxx_bgxx_canal","change_date":"2017-03-03","change_count":"","content_after_change":"王萍","change_items":"负责人变更（法定代表人、负责人、首席代表、合伙事务执行人等变更）变更","bbd_qyxx_id":"27b9479f6a304facbfb646fa9935e9a5","content_before_change":"雷运强","company_name":"石首市祥运船务有限公司","canal_eventtype":"DELETE","id":"293480397","canal_time":"2017/10/23 23:59:58","bbd_dotime":"2017-07-09","bbd_uptime":"1499599370"}"""
    val json100 = """{"canal_table":"qyxx_bgxx_canal","change_date":"2017-03-03","change_count":"","content_after_change":"e23e王萍","change_items":"负责人变更（法定代表人、负责人、首席代表、合伙事务执行人等变更）变更","bbd_qyxx_id":"27b9479f6a304facbfb646fa9935e9a5","content_before_change":"雷运强","company_name":"石首市祥运船务有限公司","canal_eventtype":"INSERT","id":"293480397","canal_time":"2017/10/23 23:59:58","bbd_dotime":"2017-07-09","bbd_uptime":"1499599370"}"""
    val json100ed = """{"canal_table":"qyxx_bgxx_canal","change_date":"2017-03-03","change_count":"","content_after_change":"e23e王萍","change_items":"负责人变更（法定代表人、负责人、首席代表、合伙事务执行人等变更）变更","bbd_qyxx_id":"27b9479f6a304facbfb646fa9935e9a5","content_before_change":"雷运强","company_name":"石首市祥运船务有限公司","canal_eventtype":"DELETE","id":"293480397","canal_time":"2017/10/23 23:59:58","bbd_dotime":"2017-07-09","bbd_uptime":"1499599370"}"""


    val json11 = """{"regno":"","esdate":"2005-04-04","no":"None","address":"陕西省渭南市蒲城县荆姚镇","create_time":"2017-09-01 18:37:03","canal_table":"qyxx_fzjg_merge_canal","enterprise_status":"存续","regno_or_creditcode":"91610526MA6Y2406XH","bbd_branch_id":"57d77bead4f541be89796cff06ff9a58","bbd_qyxx_id":"483980aeb4334ca8806f3a1c1c38e50e","company_name":"陕西凯莱医药连锁有限责任公司","name":"陕西凯莱医药连锁有限责任公司永红锁店","regorg":"蒲城县工商行政管理局","canal_eventtype":"INSERT","id":"4091837","canal_time":"2017/10/20 11:28:29","frname":"杨阳","bbd_dotime":"2017-10-18","bbd_uptime":"1508318814"}"""
    val json11ed = """{"regno":"","esdate":"2005-04-04","no":"None","address":"陕西省渭南市蒲城县荆姚镇","create_time":"2017-09-01 18:37:03","canal_table":"qyxx_fzjg_merge_canal","enterprise_status":"存续","regno_or_creditcode":"91610526MA6Y2406XH","bbd_branch_id":"57d77bead4f541be89796cff06ff9a58","bbd_qyxx_id":"483980aeb4334ca8806f3a1c1c38e50e","company_name":"陕西凯莱医药连锁有限责任公司","name":"陕西凯莱医药连锁有限责任公司永红锁店","regorg":"蒲城县工商行政管理局","canal_eventtype":"DELETE","id":"4091837","canal_time":"2017/10/20 11:28:29","frname":"杨阳","bbd_dotime":"2017-10-18","bbd_uptime":"1508318814"}"""


    val json12 = """{"resume":"","no":"","canal_table":"qyxx_baxx_canal","sex":"","type":"executive","salary":"","idno":"","name_id":"1b7f3eda845e2cca40b3977aaa263277","idtype":"","bbd_qyxx_id":"32965002481a4c31a93ccb4f880eaedf","company_name":"广西统华商贸有限公司","name":"李统政","canal_eventtype":"DELETE","id":"595495182","position":"经理","asstarting":"","canal_time":"2017/10/20 10:58:38","bbd_dotime":"2017-10-18","bbd_uptime":"1508288762"}"""
    val json13 = """{"canal_table":"qyxx_bgxx_canal","change_date":"2015-08-24","change_count":"","content_after_change":"股权转让信息：【新增】转让日期:,转让额所占投资比:40.0,转让额:40.0,币种:,受让人证照号码:*********,转让人身份标识:3502000000000000000001008264,转让类型:购买,受让人身份标识:3502000000000000000001008263,转让人转让人证照号码:*********,转让人转让人证照类型:,受让人证照类型:,转让人:危华英,受让人:林炜斌【新增】转让日期:,转让额所占投资比:30.0,转让额:30.0,币种:,受让人证照号码:*********,转让人身份标识:3502000000000000000001008263,转让类型:购买,受让人身份标识:3502000000000000000001209216,转让人转让人证照号码:*********,转让人转让人证照类型:,受让人证照类型:,转让人:周杨,受让人:江义龙企业基本信息：企业(机构)类型:有限责任公司(自然人投资或控股)投资人及出资信息：投资人类型:自然人股东,投资人:周杨,证照类型:,证照编号:************,认缴出资额:30.0,实缴出资额:0.0,出资方式:,出资比例:30.0,出资时间:,余额缴付期限:528,国别(地区):中国,是否厦门企业:,厦门法人投资序号:投资人类型:自然人股东,投资人:林炜斌,证照类型:,证照编号:******,认缴出资额:40.0,实缴出资额:0.0,出资方式:,出资比例:40.0,出资时间:,余额缴付期限:0,国别(地区):中国,是否厦门企业:,厦门法人投资序号:【新增】投资人类型:自然人股东,投资人:江义龙,证照类型:,证照编号:,认缴出资额:30.0,实缴出资额:0.0,出资方式:,出资比例:30.0,出资时间:,余额缴付期限:528,国别(地区):中国,是否厦门企业:,厦门法人投资序号:","change_items":"","bbd_qyxx_id":"89c33e3abfbc4e72beb2a15ce01db80f","content_before_change":"股权转让信息：企业基本信息：企业(机构)类型:有限责任公司(自然人投资或控股)投资人及出资信息：投资人类型:自然人股东,投资人:危华英,证照类型:,证照编号:******,认缴出资额:40.0,实缴出资额:0.0,出资方式:货币,出资比例:40.0,出资时间:,余额缴付期限:0,国别(地区):中国,是否厦门企业:,厦门法人投资序号:投资人类型:自然人股东,投资人:周杨,证照类型:,证照编号:******,认缴出资额:60.0,实缴出资额:0.0,出资方式:货币,出资比例:60.0,出资时间:,余额缴付期限:0,国别(地区):中国,是否厦门企业:,厦门法人投资序号:","company_name":"厦门市新桥工贸有限公司","canal_eventtype":"INSERT","id":"364946382","canal_time":"2017/10/25 15:12:57","bbd_dotime":"2017-10-19","bbd_uptime":"1508344720"}"""
    val json14 = """{"resume":"","no":"","canal_table":"qyxx_baxx_canal","sex":"","type":"supervisor","salary":"","idno":"","name_id":"dcf4f0e56dd003afbd353bd214c6dc78","idtype":"","bbd_qyxx_id":"b848a711cf56468e94815c826c80ddaa","company_name":"宜昌市蓉江机械设备有限公司","name":"吕蓉","canal_eventtype":"DELETE","id":"599273257","position":"监事","asstarting":"","canal_time":"2017/10/25 15:11:33","bbd_dotime":"2017-10-19","bbd_uptime":"1508344818"}"""
    val json15 = """{"create_time":"2017-10-11","bbd_qyxx_id":"c51c2348a85a466f894708c62670763c","canal_table":"black_list_canal","bbd_xgxx_id":"70172397af68d0f463fafa5ef0edee4e","company_name":"湖北蕲春三江实业有限公司","property":"1003","ctime":"2017-10-21 16:04:42","canal_eventtype":"INSERT","canal_time":"2017/11/03 00:34:49"}"""


    val json16 = """{"develop_level":"","view_rate":"","county":"","company_nature":"股份制企业","delivery_time":"","source":"智联招聘","salary":"5000-8000","job_address":"","job_nature":"全职","credit_level":"","e_mail":"","delivery_number":"","id":"262277610","job_title":"VR游戏动漫学徒会","bbd_type":"recruit","canal_time":"2017/11/0300:00:11","responsedate":"","reportto":"","create_time":"2017-10-1707:42:01","postcode":"","job_functions":"机动车司机;驾驶","salary_system":"","bbd_industry":"其他","job_descriptions":"1.VR有国家各种政策的大力支持，为家储备游戏设计人才；2.安排实训实习，直接学习工作中的经验和技术，一步到位；3.金牌单位，现学员爆满，已搬至更大空间的实训环境。4.签订协议，保障入职的安排入职要求1.高中或同等学历，18至35周岁，对游戏动漫感兴趣的人士2.品行端正、强烈的责任心与进取心、良好的抗压能力3.良好的沟通能力与团队协作能力4.思维活跃、有创新能力与创新意识者优先岗位待遇1.薪资待遇:年薪8万-30万2.利待遇:季度和年终奖、五险一金及补充医疗险、餐补、车贴和带薪年假3.入职保障:入学签就业服务协议,实训合格后100%安排入职。如果你对游戏动漫设计岗位感兴趣，如果你喜欢玩游戏，想自己开发一游戏出来挣大钱！欢迎您咨询-0基础岗前实训项目，2017年学实习生不断爆棚！机会不是一直有！","sex_required":"","company_introduction":"","company_name":"福州飞腾俊辰网络科技有限公司","education_required":"不限","jobfair_time":"","bbd_salary":"6500","canal_eventtype":"INSERT","language_required":"","recruit_numbers":"5","benefits":"五险一金,年底双薪,绩效奖金,弹性作,补充医疗保险,定期体检,员工旅游","authenticate":"","city":"","canal_table":"recruit_canal","contact_information":"","industry":"计算机软件","enscale":"100-499人","bbd_source":"智联招聘","cohr":"","agerequired":"","department":"","underling_numbers":"","bbd_uptime":"1508197165","pubdate":"2017-10-17","bbd_url":"http://jobs.zhaopin.com/515423684250498.htm","website_address":"www.ftjcvr.com","address":"","bbd_xgxx_id":"a5b51fdbdac929d434d3bfdd3af5b3ed","majors_required":"","bbd_recruit_num":"5","pubdate_doublet":"2017年10月","validdate":"","responserate":"","jobfair_location":"","location":"福州-仓山区","page_content":"","bbd_dotime":"2017-10-17","service_year":"不限"}"""
    val json16de = """{"develop_level":"","view_rate":"","county":"","company_nature":"股份制企业","delivery_time":"","source":"智联招聘","salary":"5000-8000","job_address":"","job_nature":"全职","credit_level":"","e_mail":"","delivery_number":"","id":"262277610","job_title":"VR游戏动漫学徒会","bbd_type":"recruit","canal_time":"2017/11/0300:00:11","responsedate":"","reportto":"","create_time":"2017-10-1707:42:01","postcode":"","job_functions":"机动车司机;驾驶","salary_system":"","bbd_industry":"其他","job_descriptions":"1.VR有国家各种政策的大力支持，为家储备游戏设计人才；2.安排实训实习，直接学习工作中的经验和技术，一步到位；3.金牌单位，现学员爆满，已搬至更大空间的实训环境。4.签订协议，保障入职的安排入职要求1.高中或同等学历，18至35周岁，对游戏动漫感兴趣的人士2.品行端正、强烈的责任心与进取心、良好的抗压能力3.良好的沟通能力与团队协作能力4.思维活跃、有创新能力与创新意识者优先岗位待遇1.薪资待遇:年薪8万-30万2.利待遇:季度和年终奖、五险一金及补充医疗险、餐补、车贴和带薪年假3.入职保障:入学签就业服务协议,实训合格后100%安排入职。如果你对游戏动漫设计岗位感兴趣，如果你喜欢玩游戏，想自己开发一游戏出来挣大钱！欢迎您咨询-0基础岗前实训项目，2017年学实习生不断爆棚！机会不是一直有！","sex_required":"","company_introduction":"","company_name":"福州飞腾俊辰网络科技有限公司","education_required":"不限","jobfair_time":"","bbd_salary":"6500","canal_eventtype":"DELETE","language_required":"","recruit_numbers":"5","benefits":"五险一金,年底双薪,绩效奖金,弹性作,补充医疗保险,定期体检,员工旅游","authenticate":"","city":"","canal_table":"recruit_canal","contact_information":"","industry":"计算机软件","enscale":"100-499人","bbd_source":"智联招聘","cohr":"","agerequired":"","department":"","underling_numbers":"","bbd_uptime":"1508197165","pubdate":"2017-10-17","bbd_url":"http://jobs.zhaopin.com/515423684250498.htm","website_address":"www.ftjcvr.com","address":"","bbd_xgxx_id":"a5b51fdbdac929d434d3bfdd3af5b3ed","majors_required":"","bbd_recruit_num":"5","pubdate_doublet":"2017年10月","validdate":"","responserate":"","jobfair_location":"","location":"福州-仓山区","page_content":"","bbd_dotime":"2017-10-17","service_year":"不限"}"""


    val json17 = """{"esdate":"2005-05-17","openfrom":"","company_type":"个体工商户","operating_period":"","opento":"","regcap_amount":"","type":"山西","company_province":"山西","bbd_qyxx_id":"00001f4a48c14901bea43be29f109qwe","credit_code":"","cancel_date":"","regorg":"城镇工商所","id":"","investcap_currency":"","realcap":"","bbd_type":"shanxitaiyuan","canal_time":"2017/11/03 10:14:35","company_companytype":"9300","regno":"140931610004653","approval_date":"2005-05-17","create_time":"2017-09-09 20:41:06","frname_id":"c63aff7b9ed40447c89601b500cb76c3","regcapcur":"","bbd_history_name":"[]","company_name":"保德县东关果枣门市","ipo_company":"","canal_eventtype":"INSERT","operate_scope":"糖枣、海红果销售","revoke_date":"","realcap_amount":"","frname_compid":"1","canal_table":"qyxx_basic_canal","regno_or_creditcode":"","parent_firm":"","company_enterprise_status":"存续","company_currency":"","bbd_uptime":"1503266155","frname":"张小军","address":"忻州保德县东关滨河路","investcap_amount":"","regcap_currency":"","enterprise_status":"存续（在营、开业、在册）","company_county":"140931","company_industry":"F","form":"个人经营","regcap":"","company_regorg":"140000","invest_cap":"","bbd_dotime":"2017-08-21","realcap_currency":""}"""
    val json18 = """{"esdate":"2016-10-10","openfrom":"2016-10-10","company_type":"有限责任公司（自然人投资或控股）","operating_period":"","opento":"","regcap_amount":"1000000.0","type":"四川","company_province":"四川","bbd_qyxx_id":"czv2sd1356sdf12vds51513sdf5165125d","credit_code":"914407847790885546","cancel_date":"","regorg":"成都市工商局","id":"","investcap_currency":"","realcap":"","bbd_type":"sichuan","canal_time":"2017/11/03 14:28:28","company_companytype":"1251","regno":"510599684698444","approval_date":"2016-10-10","create_time":"2017-11-03 11:09:26","frname_id":"sd5sd16ascx2d56as28a6s2x5a6sd46aa","regcapcur":"","bbd_history_name":"[]","company_name":"成都市苏打水科技有限公司","ipo_company":"","canal_eventtype":"INSERT","operate_scope":"科技","revoke_date":"","realcap_amount":"","frname_compid":"1","canal_table":"qyxx_basic_canal","regno_or_creditcode":"","parent_firm":"","company_enterprise_status":"存续","company_currency":"人民币","bbd_uptime":"1507601410","frname":"奥斯丁丁","address":"成都市武侯区天府大道11－11","investcap_amount":"","regcap_currency":"人民币","enterprise_status":"存续","company_county":"\t510107","company_industry":"F","form":"","regcap":"10000万元","company_regorg":"\t510107","invest_cap":"","bbd_dotime":"2017-10-10","realcap_currency":""}"""

    val json19 = """{"bbd_table":"qyxx_wanfang_zhuanli","create_time":"2017-11-06 10:25:00","bbd_qyxx_id":"0001094d836e44b9b0159b1222470123","canal_table":"xgxx_relation_canal","bbd_xgxx_id":"qwe987654321qwew45614645455qwe31","canal_eventtype":"INSERT","id_type":"0","id":"40","canal_time":"2017/11/07 17:34:21"}"""
    val json19ed = """{"bbd_table":"qyxx_wanfang_zhuanli","create_time":"2017-11-06 10:25:00","bbd_qyxx_id":"0001094d836e44b9b0159b1222470123","canal_table":"xgxx_relation_canal","bbd_xgxx_id":"qwe987654321qwew45614645455qwe31","canal_eventtype":"DELETE","id_type":"0","id":"40","canal_time":"2017/11/07 17:34:21"}"""

    //插入
    val result = CypherToNeo4j.getCypher(json5) //cf20d42646814390adda48367d71bbc6
    val result3 = CypherToNeo4j.getCypher(json55) //zyfcf20d42646814390adda48367d71bbc6
    val result5 = CypherToNeo4j.getCypher(json10)
    val result7 = CypherToNeo4j.getCypher(json4)
    val result9 = CypherToNeo4j.getCypher(json44)
    //删除
    val result2 = CypherToNeo4j.getCypher(json5de) //cf20d42646814390adda48367d71bbc6
    val result4 = CypherToNeo4j.getCypher(json55de) //zyfcf20d42646814390adda48367d71bbc6
    val result6 = CypherToNeo4j.getCypher(json10ed)
    val result8 = CypherToNeo4j.getCypher(json4ed)
    val result10 = CypherToNeo4j.getCypher(json44ed)
    val sss="""{"bbd_table":"recruit","create_time":"2017-12-28 13:46:41","bbd_qyxx_id":"7e9143911eff4249a06d6726bb8db7dc","canal_table":"xgxx_relation_canal","bbd_xgxx_id":"d3ae49cb8f50fe2011a802567b7b83d2","canal_eventtype":"INSERT","id_type":"0","id":"1467467663","canal_time":"2017/12/28 13:46:42:516"}"""
    val result110=CypherToNeo4j.getCypher(sss)
    result110._2.foreach(println)

//    for (i <- result7._2) {
//      println(i)
//    }
//
//    for (i <- result8._2) {
//      println(i)
//    }

    //执行多次
//    val multi_result = Array(
//      result7, result8, result7, result9, result10,
//      result10, result9
//    )
//    multi_result.map(addPerson).foreach(println)

    //执行一次
//    println(addPerson(result4))


    //    val properties = new Properties()
    //    val path = this.getClass.getClassLoader.getResource("com/bbd/bigdata/core/region_map.properties").getPath //文件要放到resource文件夹下
    //    properties.load(new FileInputStream(path))
    //    val region_info = properties.getProperty("420324")
    //    for (i <- region_info.split("\\|")){
    //      println(i)
    //    }

    //    def getRegionInfo(company_county:String): Array[String] = Try {
    //      val region_properties = new Properties()
    //      val region_path = this.getClass.getClassLoader.getResource(
    //        "com/bbd/bigdata/core/region_map.properties").getPath
    //      region_properties.load(new FileInputStream(region_path))
    //      region_properties.getProperty(company_county).split("\\|")
    //    }.recover {
    //      case e: Throwable => Array("-", "-")
    //    }.get
    //
    //    for (i <- getRegionInfo("620826")){
    //      println(i)
    //    def getCompanyProperty(alias_name: String) = {
    //      val event_properties = new Properties()
    //      val region_path = this.getClass.getClassLoader.getResource(
    //        "com/bbd/bigdata/core/event_to_company_property_map.properties").getPath
    //      event_properties.load(new FileInputStream(region_path))
    //      event_properties.values.toArray.filter(
    //        x => x != ""
    //      ).map(
    //        x => s"ON CREATE SET $alias_name.$x = 0"
    //      ).reduce(
    //        (x, y) => x+"\n"+y
    //      )
    //    }
    //    println(getCompanyProperty("a"))

  }
}
