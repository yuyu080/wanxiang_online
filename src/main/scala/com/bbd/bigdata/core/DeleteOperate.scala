package com.bbd.bigdata.core

object DeleteOperate{
  def clearBaxx(bbd_qyxx_id: String): String = {
    val str =
      s"""
         |MATCH (p)-[:SUPERVISOR|EXECUTIVE|DIRECTOR]->(r:Role)-[:SUPERVISOR|EXECUTIVE|DIRECTOR]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET c.baxx=0
         |DETACH DELETE r
       """.stripMargin
    str
  }

  def clearGdxx(bbd_qyxx_id: String): String = {
    val str =
      s"""
         |MATCH (p)-[:INVEST]->(r:Role:Invest)-[:INVEST]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET c.gdxx=0
         |SET p.dwtzxx=p.dwtzxx-1
         |DETACH DELETE r
       """.stripMargin
    str
  }

  def clearBasic(bbd_qyxx_id: String): String = {
    val str =
      s"""
         |MATCH (p)-[:LEGAL]->(r:Role:Invest)-[:LEGAL]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |DETACH DELETE r
       """.stripMargin
    str
  }


}
