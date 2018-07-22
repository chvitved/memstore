package com.memstore.entity.impl.cepb

import com.memstore.Types.Entity
import com.memstore.entity.CompactEntityMetaData
import com.memstore.serialization.Serialization.Tombstone
import com.memstore.entity.TombStone

object CEPBMetaData{
  
  def apply(name: String, notPooledcolumns: Seq[String]): CEPBMetaData = CEPBMetaData(name, Map(), Map(), Map(), Set(notPooledcolumns:_*))
  
  private def updateMetaDataAndGetIndexList(emd: CEPBMetaData, entity: Entity): (CEPBMetaData, List[(Int, Any)]) = {
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
          (CEPBMetaData(emd.name, newMap, newReverseMap, newTypeMap, emd.notPooledColumnsName), (index -> value) :: indexValueList)
        }
      }
    }
  }
  
}

case class CEPBMetaData(name: String, columnToIndexMap: Map[String, Int], reverseMap: Map[Int, String], 
    typeMap: Map[Int, Class[_]], notPooledColumnsName: Set[String]) extends CompactEntityMetaData {
  
  def getIndexesAndUpdateMetaData(e: Entity) = CEPBMetaData.updateMetaDataAndGetIndexList(this, e)
  
  def poolValue(columnId: Int): Boolean = !notPooledColumnsName.contains(indexToColumn(columnId))
  
  def getType(columnId: Int): Class[_] = typeMap(columnId)
  
  def indexToColumn(columnId: Int): String = reverseMap.getOrElse(columnId, null) 
}

