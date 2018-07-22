package com.memstore.index.factory

object OrderingFactory {

  def create(key: Any): Ordering[Any] = {
	  val ordering = key match {
	    case s: String => Ordering.String
	    case i: Int => Ordering.Int
      	case l: Long => Ordering.Long
	  }
	  ordering.asInstanceOf[Ordering[Any]]
  } 
}