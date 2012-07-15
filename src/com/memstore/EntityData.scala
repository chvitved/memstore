package com.memstore
import scala.collection.immutable.SortedMap
import java.util.Date
import com.memstore.entity.CompactEntity
import com.memstore.entity.EntityTimeline

object EntityData{
  def apply(name: String) = new EntityData(name, Map[Any, EntityTimeline]())
}

class EntityData private(val name: String, val primaryKeyIndex: Map[Any, EntityTimeline]) {
  
  def add(date: Date, entity: Map[String, Any]) : EntityData = {
    val primaryKey = EntityDescriptor.getKey(name)
    val value: Any = entity(primaryKey)
    val et = primaryKeyIndex.getOrElse(value, new EntityTimeline(name))
    new EntityData(name, primaryKeyIndex + (value -> et.add(date, entity))) 
  }
  
}	