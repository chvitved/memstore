package com.memstore
import java.util.Date
import scala.collection.immutable.SortedMap
import com.memstore.entity.CompactEntity
import com.memstore.entity.ValuePool

object EntityManager {
  def apply() = new EntityManager(Map[String, EntityData]())
  
}

class EntityManager private(val map: Map[String, EntityData]) {
  
  	def add(name: String, date: Date, entity: Map[String, Any]) : EntityManager = {
  	  val n = name.intern()
  	  val d = ValuePool.intern(date)
  	  val entityData = map.getOrElse(name, EntityData(n))
  	  new EntityManager(map + (n -> (entityData + (d, entity))))
  	}
  	
  	def get(entity: String): EntityData = {
  	  map(entity)
  	}
}