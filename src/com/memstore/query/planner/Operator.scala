package com.memstore.query.planner

import com.memstore.Types.{Entity, Index}
import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.entity.CompactEntityMetaData

object Operator {
	def getOperator(operation : String, columnName: String): Operator = {
		operation match {
		  	case "=" => EqualsOperator(columnName)
			case "==" => EqualsOperator(columnName)
			case "<" => LessThanOperator(columnName)
			case ">" => BiggerThanOperator(columnName)
		//TODO map each operator
		}
	}
}

abstract class Operator(private val columnName: String) {

	protected def makeOrderedOperation(p1: Any, p2: Any) : (Ordered[Any], Ordered[Any]) = {
		(p1, p2) match {
			case (param1: Ordered[Any], param2: Ordered[Any]) => (param1, param2) 
			case _ => throw new Exception("Could not make an ordered operation on parameters " + p1 + " and " + p2);
		}
	}

	protected def operator(param1: Any, param2: Any) : Boolean
	def predicate(e: Entity, param: Any) : Boolean = {
	  operator(e(columnName), param)
	}
	
	def indexCode(index: Index, parameter: Any, date: Date, metaData: CompactEntityMetaData) : Set[Entity]

}

case class EqualsOperator(columnName: String) extends Operator(columnName){

	override def operator(param1: Any, param2: Any) = {
		param1 == param2
	}

	override def indexCode(index: Index, parameter: Any, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
		index === (parameter, date, metaData)
	}
}

case class LessThanOperator(columnName: String) extends Operator(columnName){
	override def indexCode(index: Index, parameter: Any, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
		index < (parameter, date, metaData)
	}
 
	override def operator(param1: Any, param2: Any) = {
	  val (o1, o2) = makeOrderedOperation(param1, param2)
	  o1.compareTo(o2) == -1;	
	}
}

case class BiggerThanOperator(columnName: String) extends Operator(columnName){
	override def indexCode(index: Index, parameter: Any, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
		index > (parameter, date, metaData)
	}
 
	override def operator(param1: Any, param2: Any) = {
	  val (o1, o2) = makeOrderedOperation(param1, param2)
	  o1.compareTo(o2) == 1;	
	}
}

case class EmptyOperator() extends Operator(null){
  override def indexCode(index: Index, parameter: Any, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = null
  override def operator(param1: Any, param2: Any) = true
  override def predicate(e: Entity, param: Any) : Boolean = true
}
