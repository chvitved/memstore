package com.memstore.entity.impl.cepb
import com.memstore.entity.CompactEntityDataPool

case class CEPBDataPool(val map: Map[Class[_], CEPBPoolForType]) extends CompactEntityDataPool {

	def index(o: Any): (Int, CEPBDataPool) = {
		val pool = map.getOrElse(o.getClass, CEPBPoolForType(Map[Any, Int](), Map[Int, Any]()))
		pool.map.get(o) match {
		  case Some(index) => (index, this)
		  case None => {
			val newIndex = pool.map.size
			val newPoolMap = pool.map + (o -> newIndex)
			val newPoolReverseMap = pool.reverseMap + (newIndex -> o)
			val newMap = map + (o.getClass -> CEPBPoolForType(newPoolMap, newPoolReverseMap))
			(newIndex, CEPBDataPool(newMap))
		  }
		}
	}

	def indexToValue(index: Int, clas: Class[_]): Any = {
		val pool = map(clas)
		val v = pool.reverseMap(index)
		v
	}
}

case class CEPBPoolForType(map: Map[Any, Int], reverseMap: Map[Int, Any])