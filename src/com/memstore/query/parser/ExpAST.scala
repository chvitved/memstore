package com.memstore.query.parser

abstract case class ExpAST()

case class EmptyExpAST extends ExpAST 

case class InnerExp(exp: ExpAST, list: List[(AndOrOperator, ExpAST)]) extends ExpAST

case class LeafExp(attribute: String, op: String, param: Param) extends ExpAST




case class Param(number: Int)

object AndOrOperator {
  def stringToOperator(str: String): AndOrOperator = {
    str match {
      case "and" => AndOperator()
      case "or" => OrOperator()
    }
  } 
}
abstract sealed case class AndOrOperator
case class AndOperator extends AndOrOperator
case class OrOperator extends AndOrOperator

