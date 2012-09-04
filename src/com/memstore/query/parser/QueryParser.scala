package com.memstore.query.parser

import scala.util.parsing.combinator._

class QueryParser extends JavaTokenParsers{

  def query: Parser[QueryAST] = entity~where ^^
    {case entity~where => new QueryAST(entity, where)} 
  
  def entity: Parser[String] = "entity"~>string
  
  def string: Parser[String] = """\w+""".r
  
  def int: Parser[Int] = """\d+""".r ^^ {case intStr => intStr.toInt}
  
  def where: Parser[ExpAST] = opt("where"~>andOrExp) ^^
    {
      case Some(exp) => exp
      case None => EmptyExpAST()
    }
                                 
  def andOrExp: Parser[InnerExp] = operatorExp~rep(("and" | "or")~operatorExp) ^^
    {case exp~list => InnerExp(exp, list.map{operatorValue2Tuple => 
      (AndOrOperator.stringToOperator(operatorValue2Tuple._1), operatorValue2Tuple._2)})}

  def operatorExp: Parser[ExpAST] = string~("==" | "=" | "!=" | "<=" | "<" | ">=" | ">")~parameter ^^ 
    {case attribute~op~parameter => LeafExp(attribute, op, parameter) } |
    "("~>andOrExp<~")"
    
  def parameter: Parser[Param] = ":"~int ^^ {case tuple => Param(tuple._2)}
}
