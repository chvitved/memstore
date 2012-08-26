package com.memstore.query.planner

import com.memstore.Types.Entity
import com.memstore.query.parser.QueryAST
import com.memstore.query.parser.EmptyExpAST
import com.memstore.query.parser.AndOrOperator
import com.memstore.query.parser.InnerExp
import com.memstore.query.parser.LeafExp
import com.memstore.query.parser.ExpAST

object QueryPlanner {
  
  def plan(queryAST: QueryAST): QueryPlan = {
    planExp(queryAST.whereExp) 
    null
  }
  
  private def planExp(exp: ExpAST): ExpressionPlan = {
    exp match {
      case exp:EmptyExpAST => LeafExpPlan(exp, (entitties: Set[Entity]) => entitties, null)
      case exp:InnerExp => planInnerExp(exp)
      case exp: LeafExp => planLeafExp(exp)
    }
  }
  
  private def planInnerExp(exp:InnerExp): ExpressionPlan = {
    val first = List[(ExpressionPlan, AndOrOperator)]((planExp(exp.exp), null))
    val list = exp.list.foldLeft(first) {(list, tuple) =>
      val andOrOperator = tuple._1
      val expAST = tuple._2
      (planExp(expAST), andOrOperator) :: list
    }
    InnerExpPlan(list.reverse)
  }
  
  private def planLeafExp(exp: LeafExp) : ExpressionPlan = {
    val prop = exp.attribute
    val operator = exp.op
    val param = exp.param
    
//    val rightSide = TypesManager.getValue(exp.value2.value, attributeField.getType)
//    val operator: Operator = Operator.getOperator(exp.op)
//    val indexMethod = entity.indexes.get(attributeField.getName) match {
//      case Some(index: AnyRef) => operator.indexCode(index, rightSide)
//      case None => null
//    }
//    new AnalyzedLeafExpression(exp, operator.scanCode(attributeField, rightSide), indexMethod)
    
    null
  }

}