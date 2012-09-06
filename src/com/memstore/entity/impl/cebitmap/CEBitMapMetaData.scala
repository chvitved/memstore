package com.memstore.entity.impl.cebitmap

import com.memstore.Types.Entity
import com.memstore.ValuePool

object CEBitmapMetaData{
  
  private var map = Map[String, CEBitmapMetaData]()
  
  def getAndUpdate(entityName: String, e: Entity) : List[(Int, Any)] = {
    val emd = map.getOrElse(entityName, new CEBitmapMetaData(Map(), Map(), Map()))
    val (newEmd, indexList) =  updateMetaDataAndGetIndexList(e, emd)
    map = map + (entityName -> newEmd)
    indexList
  }
  
  private def updateMetaDataAndGetIndexList(entity: Entity, emd: CEBitmapMetaData): (CEBitmapMetaData, List[(Int, Any)]) = {
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
          val newTypeMap = emd.typeMap + (index -> value.getClass)
          (new CEBitmapMetaData(newMap, newReverseMap, newTypeMap), (index -> value) :: indexValueList)
        }
      }
    }
  }
  
  def indexToColumn(entityName: String, index: Int) = {
    map(entityName).reverseMap(index)
  }
  
  def getType(entityName: String, index: Int): Class[_] = map(entityName).typeMap(index)
}

class CEBitmapMetaData private(private val columnToIndexMap: Map[String, Int], private val reverseMap: Map[Int, String], private val typeMap: Map[Int, Class[_]])