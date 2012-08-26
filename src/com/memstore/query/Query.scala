package com.memstore.query

import com.memstore.Types.Entity
import com.memstore.EntityManager
import com.memstore.query.parser.QueryParser
import com.memstore.query.parser.QueryAST
import com.memstore.query.planner.QueryPlanner

class Query {

  def query(query: String, parameters: IndexedSeq[Any], em: EntityManager) : Set[Entity] = {
    val queryAST = parseQuery(query)
    val queryPlan = QueryPlanner.plan(queryAST)
    
    null
  }
  
  private def parseQuery(query: String): QueryAST = {
    val parser = new QueryParser()
    println("parsing query")
    val startTime = System.currentTimeMillis
    val parseResult = parser.parseAll(parser.query, query)
    if (!parseResult.successful) {  
      throw new Exception("Parse error: " + parseResult)
    }
    println("done parsing in " + (System.currentTimeMillis - startTime) + "ms")
    
    parseResult.get
  }
  
}