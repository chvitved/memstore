package com.memstore.query.planner

import com.memstore.Types.{Entity, Index}
import java.util.Date

object Operator {
	def getOperator(string : String) = {
		string match {
		case "==" => EqualsOperator
		case "<" => LessThanOperator
		case ">" => BiggerThanOperator
		//TODO map each operator
		}
	}
}

abstract class Operator() {

	def scanCode(attribute: String, parameter: Any) : Set[Entity] => Set[Entity] = {
		(data: Set[Entity]) => {
		  println("start iterating")
		  val time = System.currentTimeMillis
		  val iterator = data.filter((e: Entity) => makeOrderedOperation(e(attribute), parameter, operator))
		  println("end iterating: " + (System.currentTimeMillis - time))
		  iterator
		}
	}

	protected def makeOrderedOperation(entityValue: Any, param: Any, operation: (Ordered[Any], Ordered[Any]) => Boolean) : Boolean = {
		(entityValue, param) match {
			case (param1: Ordered[Any], param2: Ordered[Any]) => operation(param1, param2)
			case _ => throw new Exception("Could not perform and operation on parameters: " + entityValue + " and " + param + " they are have not been parsed to values that implement the Ordered interface");
		}
	}

	def operator(param1: Ordered[Any], param2: Ordered[Any]) : Boolean

	def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity]

}

object EqualsOperator extends Operator{

	override def operator(param1: Ordered[Any], param2: Ordered[Any]) = {
		param1 == param2
	}

	override def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity] = {
		() => index === (parameter, date)
	}
}

object LessThanOperator extends Operator {
	override def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity] = {
		() => index < (parameter, date)
	}
 
	override def operator(param1: Ordered[Any], param2: Ordered[Any]) = {
		param1.compareTo(param2) == -1;	
	}
}

object BiggerThanOperator extends Operator {
	override def indexCode(index: Index, parameter: Any, date: Date) : () => Set[Entity] = {
		() => index > (parameter, date)
	}
 
	override def operator(param1: Ordered[Any], param2: Ordered[Any]) = {
		param1.compareTo(param2) == 1;	
	}
}
