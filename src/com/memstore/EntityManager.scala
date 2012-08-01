package com.memstore
import java.util.Date
import scala.collection.immutable.SortedMap
import com.memstore.entity.CompactEntity
import com.memstore.Types.Entity

object EntityManager {
  def apply() = new EntityManager(Map[String, EntityData]())
  
}

class EntityManager private(val map: Map[String, EntityData]) {
  
	def addEntity(ec: EntityConfig) : EntityManager = {
	  if (map.contains(ec.name)) throw new IllegalArgumentException(String.format("entity %s is already added", ec.name))
	  new EntityManager(map + (ec.name -> EntityData(ec)))
	}
  
  	def add(name: String, date: Date, entity: Map[String, Any]) : EntityManager = {
  	  val n = ValuePool.intern(name)
  	  val d = ValuePool.intern(date)
  	  val entityData = map.getOrElse(n, EntityData(new EntityConfig(n)))
  	  new EntityManager(map + (n -> (entityData + (d, entity))))
  	}
  	
  	def get(entity: String): EntityData = {
  	  map(entity)
  	}
}