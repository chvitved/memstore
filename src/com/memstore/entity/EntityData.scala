package com.memstore.entity
import java.util.Date
import com.memstore.Types.{Entity, Index}
import com.memstore.Types.EntityTimelineWithId

object EntityData{
  def apply(ec: EntityConfig) = {
    new EntityData(ec.name, ec.key, Map[Any, EntityTimeline](), setupIndexes(ec.indexes))
  }
  
  private def setupIndexes(ics: Seq[IndexConfig[_]]) : Map[String, Index] = {
    ics.foldLeft(Map[String, Index]()) {(map, ic) =>
      map + (ic.name -> ic.emptyIndex)
    }
  }
}

case class EntityData(name: String, key: String, primaryIndex: Map[Any, EntityTimeline], indexes: Map[String, Index]) {
  
  def + (date: Date, entity: Entity) : EntityData = {
    //val id: Any = ValuePool.intern(entity(key))
    val id: Any = entity(key)
    val et = primaryIndex.getOrElse(id, EntityTimeline()) + (date, entity, name)
    val updatedIndexes = updateIndexes(date, EntityTimelineWithId(et, id), indexes)
    EntityData(name, key, primaryIndex + (id -> et), updatedIndexes) 
  }
  
  def - (date: Date, id: Any) : EntityData = {
    val et = primaryIndex(id) - date
    
    val newPrimaryIndex = primaryIndex + (id -> et)
    
    val ni = indexes.foldLeft(Map[String, Index]()) {(indexMap, tuple) =>
      val iName = tuple._1
      val i = tuple._2
      indexMap + (iName -> (i - (date, EntityTimelineWithId(et, id))))
    }
    EntityData(name, key, newPrimaryIndex, ni)
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