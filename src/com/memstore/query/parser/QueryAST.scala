package com.memstore.query.parser

class QueryAST(val entity: String, val whereExp: ExpAST) {
  
  override def toString() = {
    "entity " + entity + "\n" +
    "where " + whereExp.toString()
  }
  
}
