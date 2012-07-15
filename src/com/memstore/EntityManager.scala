package com.memstore
import java.util.Date
import scala.collection.immutable.SortedMap
import com.memstore.entity.CompactEntity

object EntityManager {
  def apply() = new EntityManager(Map[String, EntityData]())
  
}

class EntityManager private(val map: Map[String, EntityData]) {
  
  	def add(name: String, date: Date, entity: Map[String, Any]) : EntityManager = {
  	  val entityData = getEntityData(name)
  	  new EntityManager(map + (name -> entityData.add(date, entity)))
  	}
  	
  	private def getEntityData(name: String) = map.getOrElse(name, EntityData(name))
  	
  	def get(entity: String): EntityData = {
  	  map(entity)
  	}
}