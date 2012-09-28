package com.memstore.entity
import java.util.Date
import scala.collection.immutable.SortedMap
import com.memstore.Types.Entity

object EntityManager {
  def apply(): EntityManager = new EntityManager(Map[String, EntityData](), CompactEntity.emptyDataPool)  
}

case class EntityManager(map: Map[String, EntityData], dataPool: CompactEntityDataPool) {
  
	def addEntity(ec: EntityConfig) : EntityManager = {
	  if (map.contains(ec.name)) throw new IllegalArgumentException(String.format("entity %s is already added", ec.name))
	  EntityManager(map + (ec.name -> EntityData(ec)), dataPool)
	}
  
  	def add(name: String, date: Date, entity: Entity) : EntityManager = {
  	  val func = (date: Date, ed: EntityData) => ed + (date, entity, dataPool) 
  	  edit(name, date, func)
  	}
  	
  	def remove(name: String, date: Date, id: Any): EntityManager = {
  	  val func = (date: Date, ed: EntityData) => (ed - (date, id, dataPool), dataPool)
  	  edit(name, date, func)
  	}
  	
  	private def edit(name: String, date: Date, func: (Date, EntityData) => (EntityData, CompactEntityDataPool)) : EntityManager = {
  	  val ed = getEntityData(name)
  	  val (newEd, newPool) = func(date, ed)
  	  EntityManager(map + (name -> newEd), newPool)
  	}
  	
  	private def getEntityData(name: String): EntityData = {
  	  map.getOrElse(name, EntityData(new EntityConfig(name)))
  	}
  	
  	def get(entity: String, id: Any, date: Date): Option[Entity] = get(entity)(id, date, dataPool)
  	
  	def get(entity: String): EntityData = {
  	  map(entity)
  	}
  	
  	def fullScan(name: String, date: Date, predicate: Entity => Boolean): Set[Entity] = {
  	  get(name).fullScan(date, predicate, dataPool)
  	}
}