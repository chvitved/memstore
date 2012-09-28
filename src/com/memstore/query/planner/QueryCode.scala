package com.memstore.query.planner

import com.memstore.Types.{Entity, Index}
import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.query.parser.AndOrOperator
import com.memstore.query.parser.AndOperator
import com.memstore.query.parser.OrOperator
import com.memstore.entity.EntityData
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.CompactEntityDataPool

object QueryCode {
  
	val limitForUsingIndex = 100;
  
	def queryCode(exp: ExpressionPlan, ed: EntityData,  pool: CompactEntityDataPool, parameters: IndexedSeq[Any], date: Date): Set[Entity] = {
	  expCode(exp, ed, pool, parameters, date, Set[Entity](), false)
	}

	private def expCode(exp: ExpressionPlan, ed: EntityData, pool: CompactEntityDataPool, parameters: IndexedSeq[Any], date: Date, resultSoFar: Set[Entity], intersect: Boolean): Set[Entity] = {
	  exp match {
	    case e: InnerExpPlan => innerExpCode(e, ed, pool, parameters, date, resultSoFar, intersect)
	    case e: LeafExpPlan =>  leafExpCode(e, ed, pool, parameters, date, resultSoFar, intersect)
	  }
	}
	
  /*************************************************
   * 
   * Code for inner expressions
   * 
   ************************************************/
	
  private def innerExpCode(expPlan: InnerExpPlan, ed: EntityData, pool: CompactEntityDataPool, parameters: IndexedSeq[Any], date: Date, resultSoFar: Set[Entity], intersect: Boolean): Set[Entity] = {
    val groupedExpressions = group(expPlan.expressions)
    groupedExpressions.foldLeft(Set[Entity]()) {(set, ge) =>
      set.union(groupCode(ge, ed, pool, parameters, date, resultSoFar, intersect))
    }
  }
  
  private def groupCode(group: List[ExpressionPlan], ed: EntityData, pool: CompactEntityDataPool, parameters: IndexedSeq[Any], date: Date, resultSoFar: Set[Entity], intersect: Boolean): Set[Entity] = {
    val sorted = group.sort((e1, e2) => e1.score < e2.score)
    val first = expCode(sorted.head, ed, pool, parameters, date, resultSoFar, intersect)
	sorted.tail.foldLeft(first) {(set, exp) =>
		set intersect expCode(exp, ed, pool, parameters, date, resultSoFar, intersect)
	}
  }
  
  private def group(expressions: List[(ExpressionPlan, AndOrOperator)]) : List[List[ExpressionPlan]] = {
    def recursiveGroup(expressions: List[(ExpressionPlan, AndOrOperator)], accumulator: List[List[ExpressionPlan]]) : List[List[ExpressionPlan]] = {
    	expressions match {
    		case (exp, and: AndOperator) :: expTail => {
    			val l = (exp :: accumulator.head) :: accumulator.tail
    			recursiveGroup(expTail, l) 
    		}
    		case (exp, or: OrOperator) :: expTail => {
    			recursiveGroup(expTail, List[ExpressionPlan](exp) :: accumulator)
    		}
    		case Nil => accumulator
    	}
    }
    
    val firstExpList = List[ExpressionPlan](expressions.head._1)
    recursiveGroup(expressions.tail, List[List[ExpressionPlan]](firstExpList))
  }
  
  
  
  
  /************************************************
   * 
   * Code for leaf expressions
   * 
   ************************************************/
	
  private def leafExpCode(expPlan: LeafExpPlan, ed: EntityData, pool: CompactEntityDataPool, parameters: IndexedSeq[Any], date: Date, resultSoFar: Set[Entity], intersect: Boolean): Set[Entity] = {
    val parameter = parameters(expPlan.exp.param.number -1) //querie parameters start from index 1 (like sql)
    if (intersect) {
      if (limitForUsingIndex < resultSoFar.size || expPlan.scanType == FullScan()) {
        filterResultsSoFar(resultSoFar, expPlan.operator, parameter)
      } else if (expPlan.scanType == primaryKeyScan()) {
    	resultSoFar.intersect(useUniqueIndex(expPlan.operator, ed, pool, parameter, date))
      } else if(expPlan.scanType == IndexScan()) {
        resultSoFar.intersect(useIndex(expPlan, ed, pool, parameter, date))
      } else {
        throw new Exception("should not get here")
      }
    } else {
      val res: Set[Entity] = expPlan.scanType match {
        case ui: primaryKeyScan => useUniqueIndex(expPlan.operator, ed, pool, parameter, date)
        case u: IndexScan => useIndex(expPlan, ed, pool, parameter, date)
        case s: FullScan => scan(expPlan, ed, pool, parameter, date)
      }
      resultSoFar.union(res)
    }
  }
  
  private def scan(expPlan: LeafExpPlan, ed: EntityData,  pool: CompactEntityDataPool, parameter: Any, date: Date): Set[Entity] = {
    val entities = 
      for(et <- ed.primaryIndex.values;
		e <- et.get(date, ed.metaData, pool) if (expPlan.operator.predicate(e, parameter))
	  ) yield e
	  entities.toSet
  }
  
  private def useIndex(expPlan: LeafExpPlan, ed: EntityData, pool: CompactEntityDataPool, parameter: Any, date: Date): Set[Entity] = {
    val index = ed.indexes(expPlan.exp.attribute)
    expPlan.operator.indexCode(index, parameter, date, ed.metaData, pool)
  }
  
  private def useUniqueIndex(operator: Operator, ed: EntityData, pool: CompactEntityDataPool, parameter: Any, date: Date): Set[Entity] = {
    operator match {
      case op: EqualsOperator => {
        val e = for (et <- ed.primaryIndex.get(parameter); e <- et.get(date, ed.metaData, pool)) yield e
        e match {
          case Some(e) => Set[Entity](e)
          case None => Set[Entity]()
        }
        
      }
      case _ => throw new Exception("only equals are supported for primary keys")
    }
    
  }
  
  private def filterResultsSoFar(resultSoFar: Set[Entity], operator: Operator, parameter: Any) : Set[Entity] = {
    resultSoFar.filter(operator.predicate(_, parameter))
  }
}