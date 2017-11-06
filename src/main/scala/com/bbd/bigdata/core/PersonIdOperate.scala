package com.bbd.bigdata.core

import com.bbd.bigdata.util.CommonFunctions

/**
  * Created by Administrator on 2017/11/6.
  */

object PersonIdOperate {
  def getCypher(bbd_qyxx_id: String,
                old_person_id: String,
                new_person_id: String): Tuple2[String, Array[String]] = {
    (
      "",
      Array(
        s"""
           |MATCH (a:Person {bbd_qyxx_id: "$old_person_id" })-[]->(:Role)-[]->(d:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
           |MERGE (b:Person {bbd_qyxx_id: "$new_person_id" })
           |${CommonFunctions.getCompanyProperty("b")}
           |ON CREATE SET b.create_time = timestamp()
           |WITH a,b
           |MATCH (a)-[:IS]->(e:Role)-[:OF]->(c:Company)
           |WITH a,b,e,c
           |MERGE (b)-[:IS]->(e)
           |return a,b,e,c
         """.stripMargin,
        s"""
           |MATCH (a:Person {bbd_qyxx_id: "$old_person_id" }), (b:Person {bbd_qyxx_id: "$new_person_id" })
           |SET b.dwtzxx = a.dwtzxx + b.dwtzxx
           |SET b.name = a.name
           |SET b.update_time = timestamp()
           |return a,b
         """.stripMargin,
        s"""
           |MATCH (a:Person {bbd_qyxx_id: "$old_person_id" })-[]->(:Role)-[]->(d:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
           |MERGE (b:Person {bbd_qyxx_id: "$new_person_id" })
           |WITH a,b
           |MATCH (a)-[:VIRTUAL]->(e:Role:Isinvest)-[:VIRTUAL]->(c:Company)
           |WITH a,b,e,c
           |MERGE (b)-[:VIRTUAL]->(e)
           |DETACH DELETE a
           |return a,b,e,c
         """.stripMargin
      )
    )
  }
}
