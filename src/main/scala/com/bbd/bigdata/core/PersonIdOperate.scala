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
           |${CommonFunctions.getPersonProperty("b")}
           |ON CREATE SET b.create_time = timestamp()
         """.stripMargin,
        s"""
           |MATCH (b:Person {bbd_qyxx_id: "$new_person_id"}), (a:Person {bbd_qyxx_id: "$old_person_id" })-[:INVEST]->(e:Role)
           |MERGE (b)-[:INVEST]->(e)
         """.stripMargin,
        s"""
           |MATCH (b:Person {bbd_qyxx_id: "$new_person_id"}), (a:Person {bbd_qyxx_id: "$old_person_id" })-[:SUPERVISOR]->(e:Role)
           |MERGE (b)-[:SUPERVISOR]->(e)
         """.stripMargin,
        s"""
           |MATCH (b:Person {bbd_qyxx_id: "$new_person_id"}), (a:Person {bbd_qyxx_id: "$old_person_id" })-[:DIRECTOR]->(e:Role)
           |MERGE (b)-[:DIRECTOR]->(e)
         """.stripMargin,
        s"""
           |MATCH (b:Person {bbd_qyxx_id: "$new_person_id"}), (a:Person {bbd_qyxx_id: "$old_person_id" })-[:LEGAL]->(e:Role)
           |MERGE (b)-[:LEGAL]->(e)
         """.stripMargin,
        s"""
           |MATCH (b:Person {bbd_qyxx_id: "$new_person_id"}), (a:Person {bbd_qyxx_id: "$old_person_id" })-[:EXECUTIVE]->(e:Role)
           |MERGE (b)-[:EXECUTIVE]->(e)
         """.stripMargin,
        s"""
           |MATCH (a:Person {bbd_qyxx_id: "$old_person_id" }), (b:Person {bbd_qyxx_id: "$new_person_id" })
           |SET b.dwtzxx = a.dwtzxx + b.dwtzxx
           |SET b.name = a.name
           |SET b.update_time = timestamp()
         """.stripMargin,
        s"""
           |MATCH (b:Person {bbd_qyxx_id: "$new_person_id" }), (a:Person {bbd_qyxx_id: "$old_person_id" })-[]->(:Role)-[]->(d:Company {bbd_qyxx_id: "$bbd_qyxx_id" })
           |WITH a,b
           |MATCH (a)-[:VIRTUAL]->(e:Role:Isinvest)-[:VIRTUAL]->(c:Company)
           |WITH a,b,e,c
           |MERGE (b)-[:VIRTUAL]->(e)
           |DETACH DELETE a
         """.stripMargin
      )
    )
  }
}
