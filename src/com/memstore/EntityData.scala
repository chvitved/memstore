package com.memstore
import java.util.Date
import com.memstore.Types.{Entity, Index}
import com.memstore.entity.CompactEntity
import com.memstore.entity.EntityTimeline
import com.memstore.entity.ET
import com.memstore.Types.EntityTimelineWithId

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
    val id: Any = ValuePool.intern(entity(primaryKey))
    val et = primaryIndex.getOrElse(id, EntityTimeline()) + (date, entity, name)
    val updatedIndexes = updateIndexes(date, EntityTimelineWithId(et, id), indexes)
    new EntityData(name, primaryIndex + (id -> et), updatedIndexes) 
  }
  
  def - (date: Date, id: Any) : EntityData = {
    val et = primaryIndex(id) - date
    
    val newPrimaryIndex = primaryIndex + (id -> et)
    
    val ni = indexes.foldLeft(Map[String, Index]()) {(indexMap, tuple) =>
      val iName = tuple._1
      val i = tuple._2
      indexMap + (iName -> (i - (date, EntityTimelineWithId(et, id))))
    }
    new EntityData(name, newPrimaryIndex, ni)
  }
  
  def apply(id: Any): Option[Entity] = primaryIndex(id).getNow()
  
  def apply(id: Any, date: Date): Option[Entity] = primaryIndex(id).get(date)
  
  def fullScan(date: Date, predicate: Entity => Boolean): Set[Entity] = {
    //primaryIndex.values.filter(et => et.get(date) != null).map(et => et.get(date)).toSet
    //primaryIndex.values.collect{case et if(et.get(date) != null) => et.get(date)}.toSet
    //primaryIndex.values.flatMap(_.get(date)).toSet
    //primaryIndex.values.collect{case Some(et) if(et.get(date) != null) => et.get(date)}.toSet
    val es = for (et <- primaryIndex.values; e <- et.get(date); if (predicate(e))) yield e
    es.toSet
  }
  
  private def updateIndexes(date: Date, et: EntityTimelineWithId, indexes: Map[String, Index]): Map[String, Index] = {
    indexes.foldLeft(Map[String, Index]()) {(map, t) =>
      val name = t._1
      val index = t._2
      map + (name -> (index + (date, et)))
    }
  }
  
  
  
}	