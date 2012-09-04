package com.memstore.query.planner

import com.memstore.Types.Entity
import com.memstore.query.parser.AndOrOperator
import com.memstore.query.parser.LeafExp

abstract sealed case class ExpressionPlan{
  def score: Int
}
case class LeafExpPlan(exp: LeafExp, operator: Operator, scanType: ScanType) extends ExpressionPlan {
  override def score = scanType.score
}
case class InnerExpPlan(expressions: List[(ExpressionPlan, AndOrOperator)], score: Int) extends ExpressionPlan


abstract case class ScanType {
  def score: Int
}
case class primaryKeyScan extends ScanType {
  def score = 1
}
case class IndexScan extends ScanType {
  def score = 4
}
case class FullScan extends ScanType {
  def score = 16
}

  
