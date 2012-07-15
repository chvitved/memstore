package com.memstore.query.parser

import scala.util.parsing.combinator._

class QueryParser extends JavaTokenParsers{

  def query: Parser[QueryAST] = entity~where ^^
    {case entity~where => new QueryAST(entity, where)} 
  
  def entity: Parser[String] = "entity"~>string
  
  def string: Parser[String] = """\w+""".r
  
  def where: Parser[ExpAST] = opt("where"~>boolExp) ^^
    {
      case Some(exp) => exp
      case None => EmptyExpAST()
    }
                                 
  def boolExp: Parser[BoolExp] = binExp~rep(("and" | "or")~binExp) ^^
    {case exp~list => BoolExp(exp, list.map(operatorValue2Tuple => (operatorValue2Tuple._1, operatorValue2Tuple._2)))}

  def binExp: Parser[ExpAST] = string~("==" | "!=" | "<=" | "<" | ">=" | ">")~parameter ^^ 
    {case attribute~op~parameter => BinaryOpExp(attribute, op, parameter) } |
    "("~>boolExp<~")"
    
  def parameter: Parser[Param] = "?" ^^ {case param => Param()}
}
