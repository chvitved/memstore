package com.memstore.query.planner

import com.memstore.Types.Entity
import com.memstore.query.parser.ExpAST
import com.memstore.query.parser.AndOrOperator

abstract sealed case class ExpressionPlan

case class LeafExpPlan(val exp: ExpAST, val scanCode: Set[Entity] => Set[Entity], val indexCode: () => Set[Entity]) extends ExpressionPlan 
  
case class InnerExpPlan(val expressions: List[(ExpressionPlan, AndOrOperator)]) extends ExpressionPlan
  
