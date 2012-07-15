package com.memstore.query.parser

abstract case class ExpAST()

case class EmptyExpAST extends ExpAST 

case class BoolExp(exp: ExpAST, list: List[(String, ExpAST)]) extends ExpAST

case class BinaryOpExp(attribute: String, op: String, param: Param) extends ExpAST

case class ExpWithBracketsAST(exp: ExpAST) extends ExpAST {
  
  override def toString() = {
    "(" + exp + ")"
  }
}

case class Param