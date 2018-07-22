package com.memstore.entity
import java.util.Date
import com.memstore.Types.{Entity, PrimaryIndex, SecondaryIndex}
import com.memstore.util.TimeUtil

object EntityData{
  
  def apply(ec: EntityConfig) = {
    val metaData = CompactEntity.emptyMetaData(ec.name, ec.notPooledColumns)
    new EntityData(metaData, ec.key, ec.primaryIndex, setupIndexes(ec.indexes))
  }
  
  private def setupIndexes(ics: Seq[String]) : Map[String, SecondaryIndex] = {
    ics.foldLeft(Map[String, SecondaryIndex]()) {(map, ic) =>
      map + (ic -> com.memstore.index.SecondaryIndex(ic))
    }
  }
  
  private def updateSecondaryIndexes(metaData: CompactEntityMetaData, pool: CompactEntityDataPool, date: Date, id: Any, e: Entity, et: EntityTimeline, indexes: Map[String, SecondaryIndex]): Map[String, SecondaryIndex] = {
    indexes.foldLeft(Map[String, SecondaryIndex]()) {(map, t) =>
      val (name, index) = t
      val prevEntity = et.get(new Date(date.getTime() - 1), metaData, pool).map(_._1) //get the value before the current insert
      map + (name -> (index + (date, id, e, prevEntity)))
    }
  }
  
  private def removeFromSecondaryIndexes(metaData: CompactEntityMetaData, pool: CompactEntityDataPool, date: Date, id: Any, e: Entity, indexes: Map[String, SecondaryIndex]): Map[String, SecondaryIndex] = {
    indexes.foldLeft(Map[String, SecondaryIndex]()) {(map, tuple) =>
      val iName = tuple._1
      val i = tuple._2
      map + (iName -> (i - (date, id, e))) 
    }
  }
  
  private def validate(e: Entity, key: String) {
    val id = e.getOrElse(key, null)
    if (id == null) {
      throw new Exception("key value cannot be null. Key column is %s and entity is %s".format(key, e)) 
    }
  }
}

case class EntityData(metaData: CompactEntityMetaData, key: String, primaryIndex: PrimaryIndex, indexes: Map[String, SecondaryIndex]) {
  
  def + (date: Date, entity: Entity, pool: CompactEntityDataPool) : (EntityData, CompactEntityDataPool)  = {
	val e = entity.filter(_._2 != null) //remove all entries with null values
    EntityData.validate(e, key)
	val id = e(key)
    val result = primaryIndex(id) + (date, e, metaData, pool)
    if (result.changed) { //maybe just remove this if
    	val newEt = result.et
    	val newMetaData = result.ceMetaData;
    	val newPool = result.pool
    	val updatedIndexes = EntityData.updateSecondaryIndexes(newMetaData, newPool, date, id, e, newEt, indexes)
    	(EntityData(newMetaData, key, primaryIndex + (id -> newEt), updatedIndexes), newPool)  
    } else (this, pool)
  }
  
  def - (date: Date, id: Any, pool: CompactEntityDataPool) : EntityData = {
    val et = primaryIndex(id) 
    val newEt = et - date
    val newPrimaryIndex = primaryIndex + (id -> newEt)
    val ni = EntityData.removeFromSecondaryIndexes(metaData, pool, date, id, et.get(date, metaData, pool).get._1, indexes)
    EntityData(metaData, key, newPrimaryIndex, ni)
  }
  
  def apply(id: Any, pool: CompactEntityDataPool): Option[Entity] = apply(id, new Date(), pool)
  
  def apply(id: Any, date: Date, pool: CompactEntityDataPool): Option[Entity] = getWithInsertionDate(id, date, pool).map(_._1)
  
  def getWithInsertionDate(id: Any, date: Date, pool: CompactEntityDataPool): Option[(Entity, Date)] = primaryIndex(id).get(date, metaData, pool)
  
  def fullScan(date: Date, predicate: Entity => Boolean, pool: CompactEntityDataPool): Set[Entity] = {
    //primaryPrimaryIndex.values.filter(et => et.get(date) != null).map(et => et.get(date)).toSet
    //primaryPrimaryIndex.values.collect{case et if(et.get(date) != null) => et.get(date)}.toSet
    //primaryPrimaryIndex.values.flatMap(_.get(date)).toSet
    //primaryPrimaryIndex.values.collect{case Some(et) if(et.get(date) != null) => et.get(date)}.toSet
    val es = for (et <- primaryIndex.values; e <- et.get(date, metaData, pool).map(_._1); if (predicate(e))) yield e
    es.toSet //TODO this could be done more efficient
  }
  
}	