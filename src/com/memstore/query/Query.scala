package com.memstore.query

import com.memstore.Types.Entity
import com.memstore.entity.EntityManager
import com.memstore.query.parser.QueryParser
import com.memstore.query.parser.QueryAST
import com.memstore.query.planner.QueryPlanner
import com.memstore.query.planner.QueryCode
import java.util.Date
import com.memstore.util.TimeUtil

object Query {
  def apply(queryString: String, parameters: IndexedSeq[Any], em: EntityManager, date: Date) : Set[Entity] = { 
    query(queryString, parameters, em, date)
  }
    
  def query(query: String, parameters: IndexedSeq[Any], em: EntityManager, date: Date) : Set[Entity] = {
    println()
    println(query)
    val startTime = System.nanoTime
    val queryAST = parseQuery(query)
    val ed = em.get(queryAST.entity)
    val queryPlan = QueryPlanner.plan(queryAST, ed)
    val startQueryCodeTime = System.nanoTime
    val res = QueryCode.queryCode(queryPlan, ed, em.dataPool, parameters, date)
    val doneTime = System.nanoTime
    println("done queriyng in " + TimeUtil.printNanos(doneTime - startTime))
    println("querycode took " + TimeUtil.printNanos(doneTime - startQueryCodeTime))
    res
  }
  
  private def parseQuery(query: String): QueryAST = {
    val parser = new QueryParser()
    val startTime = System.nanoTime
    val parseResult = parser.parseAll(parser.query, query)
    if (!parseResult.successful) {  
      throw new Exception("Parse error: " + parseResult)
    }
    println("done parsing in " + TimeUtil.printNanos(System.nanoTime - startTime))
    parseResult.get
  }
}

