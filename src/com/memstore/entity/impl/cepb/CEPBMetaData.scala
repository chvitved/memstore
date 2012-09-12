package com.memstore.entity.impl.cepb

import com.memstore.Types.Entity
import com.memstore.EntityConfig

object CEPBMetaData{
  
  private var map = Map[Int, CEPBMetaData]()
  
  def configNotPooledColumns(entityName: String, columns: String*) {
   val entityId = EntityNameMetaData.getAndUpdate(entityName)
   if (map.contains(entityId)) {
     throw new Exception("It is only possible to configure columns to not be pooled before any entities are created")
   } else {
     val newEmd = new CEPBMetaData(Map(), Map(), Map(), Set(columns:_*), Set()) 
     updateMap(entityId, newEmd)
   }
  }
  
  def poolValue(entityId: Int, columnId: Int): Boolean = !map(entityId).notPooledColumnsIndex.contains(columnId)
  
  def getAndUpdate(entityName: String, e: Entity) : List[(Int, Any)] = {
    val entityId = EntityNameMetaData.getAndUpdate(entityName)
    
    val emd = map.getOrElse(entityId, new CEPBMetaData(Map(), Map(), Map(), Set(), Set()))
    val (newEmd, indexList) = updateMetaDataAndGetIndexList(e, emd)
    updateMap(entityId, newEmd)
    indexList
  }
  
  def indexToColumn(entityIndex: Int, index: Int): String = {
    map(entityIndex).reverseMap(index)
  }
  
  def intToEntityName(index: Int) = EntityNameMetaData.intToEntityName(index)
  def entityNameToIndex(entityName: String) = EntityNameMetaData.entityNameToIndex(entityName)
  
  def getType(entityId: Int, index: Int): Class[_] = map(entityId).typeMap(index)

  private def updateMap(entityId: Int, emd: CEPBMetaData) {
    map = map + (entityId -> emd)
  }
  
  
  private def updateMetaDataAndGetIndexList(entity: Entity, emd: CEPBMetaData): (CEPBMetaData, List[(Int, Any)]) = {
    val filteredMap = entity.filter(t => !(t._2 == null))
    val start = (emd, List[(Int, Any)]())
    filteredMap.foldRight(start) { (entityTuple, accTuple) =>
      val name = entityTuple._1
      val value = entityTuple._2
      val emd = accTuple._1
      val indexValueList = accTuple._2 
      emd.columnToIndexMap.get(name) match {
        case Some(index) => (emd, (index -> value) :: indexValueList)
        case None => {
          val index = emd.columnToIndexMap.size
          val newMap = emd.columnToIndexMap + (name -> index)
          val newReverseMap = emd.reverseMap + (index -> name)
          val newTypeMap = emd.typeMap + (index -> value.getClass)
          val newNotPooledColumnsIndex = if(emd.notPooledColumnsName.contains(name)) {
            emd.notPooledColumnsIndex + index
          } else {
            emd.notPooledColumnsIndex
          }
          (CEPBMetaData(newMap, newReverseMap, newTypeMap, emd.notPooledColumnsName, newNotPooledColumnsIndex), (index -> value) :: indexValueList)
        }
      }
    }
  }
  
  private object EntityNameMetaData {
	  private var nameToInt: Map[String, Int] = Map() 
	  private var intToName: Map[Int, String] = Map()
  
	  def getAndUpdate(entityName: String): Int = {
	    if (!nameToInt.contains(entityName)) {
	      val index = nameToInt.size
	      nameToInt = nameToInt + (entityName -> index)
	      intToName = intToName + (index -> entityName)
	    }
	    nameToInt(entityName)
	  }
	  
	  def intToEntityName(index: Int): String = intToName(index) 
	  def entityNameToIndex(entityName: String) = nameToInt(entityName)
  }
  
}

case class CEPBMetaData(columnToIndexMap: Map[String, Int], reverseMap: Map[Int, String], 
    typeMap: Map[Int, Class[_]], notPooledColumnsName: Set[String], notPooledColumnsIndex: Set[Int])

