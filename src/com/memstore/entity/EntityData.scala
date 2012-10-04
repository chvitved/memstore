package com.memstore.entity
import java.util.Date
import com.memstore.Types.{Entity, Index}
import com.memstore.Types.EntityTimelineWithId

object EntityData{
  def apply(ec: EntityConfig) = {
    val metaData = CompactEntity.emptyMetaData(ec.name, ec.notPooledColumns)
    new EntityData(metaData, ec.key, Map[Any, EntityTimeline](), setupIndexes(ec.indexes))
  }
  
  private def setupIndexes(ics: Seq[IndexConfig[_]]) : Map[String, Index] = {
    ics.foldLeft(Map[String, Index]()) {(map, ic) =>
      map + (ic.name -> ic.emptyIndex)
    }
  }
  
  private def updateIndexes(metaData: CompactEntityMetaData, pool: CompactEntityDataPool, date: Date, et: EntityTimelineWithId, indexes: Map[String, Index]): Map[String, Index] = {
    indexes.foldLeft(Map[String, Index]()) {(map, t) =>
      val name = t._1
      val index = t._2
      map + (name -> (index + (date, et, metaData, pool)))
    }
  }
}

case class EntityData(metaData: CompactEntityMetaData, key: String, primaryIndex: Map[Any, EntityTimeline], indexes: Map[String, Index]) {
  
  def + (date: Date, entity: Entity, pool: CompactEntityDataPool) : (EntityData, CompactEntityDataPool)  = {
    //val id: Any = ValuePool.intern(entity(key))
    val id: Any = entity(key)
    val result = primaryIndex.getOrElse(id, EntityTimeline()) + (date, entity, metaData, pool)
    if (result.changed) { //maybe just remove this if
    	val newEt = result.et
    	val newMetaData = result.ceMetaData;
    	val newPool = result.pool
    	val updatedIndexes = EntityData.updateIndexes(newMetaData, newPool, date, EntityTimelineWithId(newEt, id), indexes)
    	(EntityData(newMetaData, key, primaryIndex + (id -> newEt), updatedIndexes), newPool)  
    } else (this, pool)
  }
  
  def - (date: Date, id: Any, pool: CompactEntityDataPool) : EntityData = {
    val et = primaryIndex(id) - date
    
    val newPrimaryIndex = primaryIndex + (id -> et)
    
    val ni = indexes.foldLeft(Map[String, Index]()) {(indexMap, tuple) =>
      val iName = tuple._1
      val i = tuple._2
      indexMap + (iName -> (i - (date, EntityTimelineWithId(et, id), metaData, pool)))
    }
    EntityData(metaData, key, newPrimaryIndex, ni)
  }
  
  def apply(id: Any, pool: CompactEntityDataPool): Option[Entity] = primaryIndex(id).getNow(metaData, pool)
  
  def apply(id: Any, date: Date, pool: CompactEntityDataPool): Option[Entity] = primaryIndex(id).get(date, metaData, pool)
  
  def fullScan(date: Date, predicate: Entity => Boolean, pool: CompactEntityDataPool): Set[Entity] = {
    //primaryIndex.values.filter(et => et.get(date) != null).map(et => et.get(date)).toSet
    //primaryIndex.values.collect{case et if(et.get(date) != null) => et.get(date)}.toSet
    //primaryIndex.values.flatMap(_.get(date)).toSet
    //primaryIndex.values.collect{case Some(et) if(et.get(date) != null) => et.get(date)}.toSet
    val es = for (et <- primaryIndex.values; e <- et.get(date, metaData, pool); if (predicate(e))) yield e
    es.toSet //TODO this could be done more efficient
  }
  
}	