package com.memstore.entity

import com.memstore.Types.Entity
import com.memstore.ValuePool

object EntityMetaData{
  
  private var map = Map[String, EntityMetaData]()
  
  def getAndUpdate(entityName: String, e: Entity) : List[(Int, Any)] = {
    val emd = map.getOrElse(entityName, new EntityMetaData(Map(), Map()))
    val (newEmd, indexList) =  updateMetaDataAndGetIndexList(e, emd)
    map = map + (entityName -> newEmd)
    indexList
  }
  
  private def updateMetaDataAndGetIndexList(entity: Entity, emd: EntityMetaData): (EntityMetaData, List[(Int, Any)]) = {
    val filteredMap = entity.filter(t => !(t._2 == null))
    val start = (emd, List[(Int, Any)]())
    filteredMap.foldLeft(start) { (accTuple, entityTuple) =>
      val name = entityTuple._1
      val value = entityTuple._2
      val emd = accTuple._1
      val indexValueList = accTuple._2 
      emd.columnToIndexMap.get(name) match {
        case Some(index) => (emd, (index -> value) :: indexValueList)
        case None => {
          val index = ValuePool.intern(emd.columnToIndexMap.size)
          val newMap = emd.columnToIndexMap + (ValuePool.intern(name) -> index)
          val newReverseMap = emd.reverseMap + (index -> name)
          (new EntityMetaData(newMap, newReverseMap), (index -> value) :: indexValueList)
        }
      }
    }
  }
  
  def indexToColumn(entityName: String, index: Int) = {
    map(entityName).reverseMap(index)
  }
}

class EntityMetaData private(private val columnToIndexMap: Map[String, Int], private val reverseMap: Map[Int, String])