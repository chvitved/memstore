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
  
  	def add(name: String, date: Date, entity: Entity) : EntityManager = {
  	  val func = (date: Date, ed: EntityData) => ed + (date, entity) 
  	  edit(name, date, func)
  	}
  	
  	def remove(name: String, date: Date, id: Any): EntityManager = {
  	  val func = (date: Date, ed: EntityData) => ed - (date, id)
  	  edit(name, date, func)
  	}
  	
  	private def edit(name: String, date: Date, func: (Date, EntityData) => EntityData) : EntityManager = {
  	  val n = ValuePool.intern(name)
  	  val d = ValuePool.intern(date)
  	  val ed = getEntityData(n)
  	  new EntityManager(map + (n -> func(d, ed)))
  	}
  	
  	private def getEntityData(name: String): EntityData = {
  	  map.getOrElse(name, EntityData(new EntityConfig(name)))
  	}
  	
  	def get(entity: String, id: Any, date: Date): Option[Entity] = get(entity)(id, date)
  	
  	def get(entity: String): EntityData = {
  	  map(entity)
  	}
  	
  	def fullScan(name: String, date: Date, predicate: Entity => Boolean): Set[Entity] = {
  	  get(name).fullScan(date, predicate)
  	}
}