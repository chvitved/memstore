package com.memstore
import scala.collection.immutable.SortedMap
import java.util.Date
import com.memstore.Types.Entity
import com.memstore.entity.CompactEntity
import com.memstore.entity.EntityTimeline
import com.memstore.index.Index

object EntityData{
  def apply(name: String) = new EntityData(name, Map[Any, EntityTimeline]())
}

class EntityData private(val name: String, val primaryIndex: Map[Any, EntityTimeline]) {
  
  def + (date: Date, entity: Entity) : EntityData = {
    val primaryKey = "_id"
    val value: Any = ValuePool.intern(entity(primaryKey))
    val et = primaryIndex.getOrElse(value, new EntityTimeline(name)) + (date, entity)
    new EntityData(name, primaryIndex + (value -> et)) 
  }
  
}	