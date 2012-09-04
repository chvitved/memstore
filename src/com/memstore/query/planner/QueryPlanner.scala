package com.memstore.query.planner

import com.memstore.Types.Entity
import com.memstore.query.parser.QueryAST
import com.memstore.query.parser.EmptyExpAST
import com.memstore.query.parser.AndOrOperator
import com.memstore.query.parser.InnerExp
import com.memstore.query.parser.LeafExp
import com.memstore.query.parser.ExpAST
import com.memstore.query.parser.AndOperator
import com.memstore.query.parser.OrOperator
import com.memstore.EntityData
import java.util.Date

object QueryPlanner {
  
  def plan(queryAST: QueryAST, ed: EntityData): ExpressionPlan = {
    planExp(queryAST.whereExp, ed) 
  }
  
  private def planExp(exp: ExpAST, ed: EntityData): ExpressionPlan = {
    exp match {
      case exp:EmptyExpAST => LeafExpPlan(null, EmptyOperator(), FullScan())
      case exp:InnerExp => planInnerExp(exp, ed)
      case exp: LeafExp => planLeafExp(exp, ed)
    }
  }
  
  private def planInnerExp(exp:InnerExp, ed: EntityData): ExpressionPlan = {
    val first = List[(ExpressionPlan, AndOrOperator)]((planExp(exp.exp, ed), null))
    val list = exp.list.foldLeft(first) {(list, tuple) =>
      val andOrOperator = tuple._1
      val expAST = tuple._2
      (planExp(expAST, ed), andOrOperator) :: list
    }
    val expList = list.reverse
    val firstScore = expList.head._1.score
    val score = expList.tail.foldLeft(firstScore) {(score, expTuple) =>
      val exp = expTuple._1
      val operator = expTuple._2
      operator match {
        case op: AndOperator => Math.min(score, exp.score)
        case op: OrOperator => score + exp.score
      }
    }
    InnerExpPlan(expList, score)
  }
  
  private def planLeafExp(exp: LeafExp, ed: EntityData) : ExpressionPlan = {
    val operator = Operator.getOperator(exp.op, exp.attribute)
    LeafExpPlan(exp, operator, findScanType(exp, ed))
  }
  
  private def findScanType(exp: LeafExp, ed: EntityData): ScanType = {
    if (ed.key == exp.attribute) {
      primaryKeyScan()
    } else {
      ed.indexes.get(exp.attribute) match {
	      case Some(index) => IndexScan() 
	      case None => FullScan()
      }
    }
  }
}