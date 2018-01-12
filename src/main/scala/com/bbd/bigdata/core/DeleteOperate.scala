package com.bbd.bigdata.core

object DeleteOperate{
  def clearBaxx(bbd_qyxx_id: String): Array[String] = {
    val str =
      s"""
         |MATCH (p)-[:SUPERVISOR|EXECUTIVE|DIRECTOR]->(r:Role)-[:SUPERVISOR|EXECUTIVE|DIRECTOR]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET p.update_time = timestamp()
         |SET c.update_time = timestamp()
         |WITH p,r,c
         |MATCH (p)-[:SUPERVISOR|EXECUTIVE|DIRECTOR]->(r)-[:SUPERVISOR|EXECUTIVE|DIRECTOR]->(c)
         |SET c.baxx=0
         |DETACH DELETE r
         |WITH p,c
         |MATCH (p)-[:VIRTUAL]->(rr:Role)-[:VIRTUAL]->(c)
         |WHERE NOT (p)-[:LEGAL|INVEST|BRANCH]->(:Entity:Role)-[:LEGAL|INVEST|BRANCH]->(c)
         |DETACH DELETE rr
       """.stripMargin
    Array(str)
  }

  def clearGdxx(bbd_qyxx_id: String): Array[String] = {
    val str1 =
      s"""
         |MATCH (p)-[:INVEST]->(r:Role:Invest)-[:INVEST]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET p.update_time = timestamp()
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
         |WHERE NOT (p)-[:LEGAL|SUPERVISOR|EXECUTIVE|DIRECTOR|BRANCH]->(:Entity:Role)-[:LEGAL|SUPERVISOR|EXECUTIVE|DIRECTOR|BRANCH]->(c)
         |SET p.update_time = timestamp()
         |SET c.update_time = timestamp()
         |DETACH DELETE r
       """.stripMargin
    Array(str1, str2)
  }

  def clearBasic(bbd_qyxx_id: String): Array[String] = {
    val str =
      s"""
         |MATCH (p)-[:LEGAL]->(r:Role)-[:LEGAL]->(c:Company {bbd_qyxx_id:"$bbd_qyxx_id"})
         |SET p.update_time = timestamp()
         |SET c.update_time = timestamp()
         |WITH p,r,c
         |MATCH (r)
         |DETACH DELETE r
         |WITH p,c
         |MATCH (p)-[:VIRTUAL]->(rr:Role)-[:VIRTUAL]->(c)
         |WHERE NOT (p)-[:SUPERVISOR|EXECUTIVE|DIRECTOR|BRANCH|INVEST]->(:Entity:Role)-[:SUPERVISOR|EXECUTIVE|DIRECTOR|BRANCH|INVEST]->(c)
         |DETACH DELETE rr
       """.stripMargin
    Array(str)
  }


}
