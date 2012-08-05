package com.memstore
import scala.collection.immutable.SortedMap
import java.util.Date
import com.memstore.Types.{Entity, Index}
import com.memstore.entity.CompactEntity
import com.memstore.entity.EntityTimeline

object EntityData{
  def apply(ec: EntityConfig) = {
    new EntityData(ec.name, Map[Any, EntityTimeline](), setupIndexes(ec.indexes))
  }
  
  private def setupIndexes(ics: Seq[IndexConfig[_]]) : Map[String, Index] = {
    ics.foldLeft(Map[String, Index]()) {(map, ic) =>
      map + (ic.name -> ic.emptyIndex)
    }
  }
}

class EntityData private(name: String, val primaryIndex: Map[Any, EntityTimeline], val indexes: Map[String, Index]) {
  
  def + (date: Date, entity: Entity) : EntityData = {
    val primaryKey = "_id"
    val value: Any = ValuePool.intern(entity(primaryKey))
    val et = primaryIndex.getOrElse(value, new EntityTimeline(name, value)) + (date, entity)
    val updatedIndexes = updateIndexes(date, et, indexes)
    new EntityData(name, primaryIndex + (value -> et), updatedIndexes) 
  }
  
  def - (date: Date, id: Any) : EntityData = {
    val et = primaryIndex(id) - date
    
    val newPrimaryIndex = primaryIndex + (id -> et)
    
    val ni = indexes.foldLeft(Map[String, Index]()) {(indexMap, tuple) =>
      val iName = tuple._1
      val i = tuple._2
      indexMap + (iName -> (i - (date, et)))
    }
    new EntityData(name, newPrimaryIndex, ni)
  }
  
  def apply(id: Any): Entity = primaryIndex(id).getNow()
  
  def apply(id: Any, date: Date): Entity = primaryIndex(id).get(date)
  
  private def updateIndexes(date: Date, et: EntityTimeline, indexes: Map[String, Index]): Map[String, Index] = {
    indexes.foldLeft(Map[String, Index]()) {(map, t) =>
      val name = t._1
      val index = t._2
      map + (name -> (index + (date, et)))
    }
  }
  
  
  
}	