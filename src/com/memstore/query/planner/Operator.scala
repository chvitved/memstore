package com.memstore.query.planner

import com.memstore.Types.{Entity, Index}
import java.util.Date

object Operator {
	def getOperator(operation : String, columnName: String, parameterIndex: Int): Operator = {
		operation match {
			case "==" => EqualsOperator(columnName, parameterIndex)
			case "<" => LessThanOperator(columnName, parameterIndex)
			case ">" => BiggerThanOperator(columnName, parameterIndex)
		//TODO map each operator
		}
	}
}

abstract class Operator(private val columnName: String, private val paramIndex: Int) {

	protected def makeOrderedOperation(p1: Any, p2: Any) : (Ordered[Any], Ordered[Any]) = {
		(p1, p2) match {
			case (param1: Ordered[Any], param2: Ordered[Any]) => (param1, param2) 
			case _ => throw new Exception("Could not make an ordered operation on parameters " + p1 + " and " + p2);
		}
	}

	def operator(param1: Any, param2: Any) : Boolean

	def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity]
	
	def predicate : (Entity, Array[Any]) => Boolean = {
	    (e: Entity, params: Array[Any]) => operator(e(columnName), params(paramIndex))
	}

}

case class EqualsOperator(columnName: String, paramIndex: Int) extends Operator(columnName, paramIndex){

	override def operator(param1: Any, param2: Any) = {
		param1 == param2
	}

	override def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity] = {
		() => index === (parameter, date)
	}
}

case class LessThanOperator(columnName: String, paramIndex: Int) extends Operator(columnName, paramIndex){
	override def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity] = {
		() => index < (parameter, date)
	}
 
	override def operator(param1: Any, param2: Any) = {
	  val (o1, o2) = makeOrderedOperation(param1, param2)
	  o1.compareTo(o2) == -1;	
	}
}

case class BiggerThanOperator(columnName: String, paramIndex: Int) extends Operator(columnName, paramIndex){
	override def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity] = {
		() => index > (parameter, date)
	}
 
	override def operator(param1: Any, param2: Any) = {
	  val (o1, o2) = makeOrderedOperation(param1, param2)
	  o1.compareTo(o2) == 1;	
	}
}
