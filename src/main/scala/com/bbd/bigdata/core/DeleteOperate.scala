package com.bbd.bigdata.core

object DeleteOperate{
  def clearBaxx(bbd_qyxx_id: String): Array[String] = {
    val str =
      s"""
         |MATCH (p)-[:SUPERVISOR|EXECUTIVE|DIRECTOR|VIRTUAL]->(r:Role)-[:SUPERVISOR|EXECUTIVE|DIRECTOR|VIRTUAL]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET p.update_time = timestamp()
         |SET r.update_time = timestamp()
         |SET c.update_time = timestamp()
         |WITH p,r,c
         |MATCH (p)-[:SUPERVISOR|EXECUTIVE|DIRECTOR|VIRTUAL]->(r)-[:SUPERVISOR|EXECUTIVE|DIRECTOR|VIRTUAL]->(c)
         |SET c.baxx=0
         |DETACH DELETE r
       """.stripMargin
    Array(str)
  }

  def clearGdxx(bbd_qyxx_id: String): Array[String] = {
    val str1 =
      s"""
         |MATCH (p)-[:INVEST]->(r:Role:Invest)-[:INVEST]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET p.update_time = timestamp()
         |SET r.update_time = timestamp()
         |SET c.update_time = timestamp()
         |WITH p,r,c
         |MATCH (p)-[:INVEST]->(r)-[:INVEST]->(c)
         |SET c.gdxx = 0
         |SET p.dwtzxx = p.dwtzxx - 1
         |DETACH DELETE r
       """.stripMargin
    val str2 =
      s"""
         |MATCH (p)-[:VIRTUAL]->(r:Role)-[:VIRTUAL]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET p.update_time = timestamp()
         |SET r.update_time = timestamp()
         |SET c.update_time = timestamp()
         |WITH p,r,c
         |MATCH (r)
         |DETACH DELETE r
       """.stripMargin
    Array(str1, str2)
  }

  def clearBasic(bbd_qyxx_id: String): Array[String] = {
    val str =
      s"""
         |MATCH (p)-[:LEGAL|VIRTUAL]->(r:Role)-[:LEGAL|VIRTUAL]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET p.update_time = timestamp()
         |SET r.update_time = timestamp()
         |SET c.update_time = timestamp()
         |WITH p,r,c
         |MATCH (r)
         |DETACH DELETE r
       """.stripMargin
    Array(str)
  }


}
