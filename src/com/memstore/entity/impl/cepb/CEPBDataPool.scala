package com.memstore.entity.impl.cepb
import com.memstore.entity.CompactEntityDataPool
import com.memstore.entity.TombStone

import java.util.ArrayList
import scala.collection.JavaConversions._

case class CEPBDataPool(val map: Map[Class[_], CEPBPoolForType]) extends CompactEntityDataPool {

	def index(o: Any): (Int, CEPBDataPool) = {
		if (o == TombStone) (-1, this)
		else {
		  val pool = map.getOrElse(o.getClass, CEPBPoolForType(new gnu.trove.map.hash.TObjectIntHashMap[Any](), new ArrayList[Any]()))
		  val index = pool.map.get(o) 
		  if (pool.map.contains(o) ) (pool.map.get(o) , this)
		  else {
				val newIndex = pool.map.size
				pool.map.put(o, newIndex)
				pool.reverseMap.add(o)
				val newMap = map + (o.getClass -> pool)
				(newIndex, CEPBDataPool(newMap))
		  }
		}
	}

	def indexToValue(index: Int, clas: Class[_]): Any = {
	  if (index == -1) TombStone
	  else {
		map(clas).reverseMap.get(index)
	  }
	}
}

/**
 * I should use immutable datastructures EVERYWHERE!
 * 
 * But this datapool is only increasing (I think it is called a monotome increasing function in mathematics)
 * 
 * So because these mutable datastructures a much more space efficient.  
 * 
 * TODO experiment with finding more space efficient implementations than the stand java ones
 */
case class CEPBPoolForType(map: gnu.trove.map.hash.TObjectIntHashMap[Any], reverseMap: ArrayList[Any])