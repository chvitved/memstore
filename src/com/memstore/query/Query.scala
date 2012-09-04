package com.memstore.query

import com.memstore.Types.Entity
import com.memstore.EntityManager
import com.memstore.query.parser.QueryParser
import com.memstore.query.parser.QueryAST
import com.memstore.query.planner.QueryPlanner
import com.memstore.query.planner.QueryCode
import java.util.Date

object Query {
  def apply(queryString: String, parameters: IndexedSeq[Any], em: EntityManager, date: Date) : Set[Entity] = { 
    query(queryString, parameters, em, date)
  }
    
  def query(query: String, parameters: IndexedSeq[Any], em: EntityManager, date: Date) : Set[Entity] = {
    val startTime = System.nanoTime
    val queryAST = parseQuery(query)
    val ed = em.get(queryAST.entity)
    val queryPlan = QueryPlanner.plan(queryAST, ed)
    val startQueryCodeTime = System.nanoTime
    val res = QueryCode.queryCode(queryPlan, ed, parameters, date)
    val doneTime = System.nanoTime
    println("done queriyng in " + (doneTime - startTime) + "nanos")
    println("querycode took " + (doneTime - startQueryCodeTime) + "nanos")
    res
  }
  
  private def parseQuery(query: String): QueryAST = {
    val parser = new QueryParser()
    val startTime = System.nanoTime
    val parseResult = parser.parseAll(parser.query, query)
    if (!parseResult.successful) {  
      throw new Exception("Parse error: " + parseResult)
    }
    println("done parsing in " + (System.nanoTime - startTime) + "nanos")
    
    parseResult.get
  }
}

