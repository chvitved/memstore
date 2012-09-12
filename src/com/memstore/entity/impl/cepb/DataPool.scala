package com.memstore.entity.impl.cepb

object DataPool {
	var map = Map[Class[_], DataPool]()

	def index[T](o: T): Int = {
		val dataPool = map.getOrElse(o.getClass, DataPool(Map(), Map()))
		dataPool.map.get(o) match {
		  case Some(index) => index
		  case None => {
			val newIndex = dataPool.map.size
			val newMap = dataPool.map + (o -> newIndex)
			val newReverseMap = dataPool.reverseMap + (newIndex -> o)
			map = map + (o.getClass -> new DataPool(newMap, newReverseMap))
			newIndex
		  }
		}
	}

	def indexToValue(index: Int, clas: Class[_]): Any = {
		val dp = map(clas)
		val v = dp.reverseMap(index)
		v
	}
}

case class DataPool(map: Map[Any, Int], reverseMap: Map[Int, Any])