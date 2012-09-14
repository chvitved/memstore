package com.memstore.entity.impl.cepb

import scala.collection.JavaConversions._
import com.memstore.entity.CompactEntity
import com.memstore.Types.Entity
import com.memstore.serialization.Serialization.PBEntity
import java.util.Arrays

object CEPB {
  
  var count: Int = 0
  var sum: Int = 0
  
  def apply(entityName: String, e: Entity): CEPB = {
    val indexValueTuples = CEPBMetaData.getAndUpdate(entityName, e)
    
    //is this necessary. CEPBMetaData.getAndUpdate could do it
    val sortedValues = indexValueTuples.toArray.sortWith{(v1, v2) => v1._1 < v2._1} 
    
    val entityId = CEPBMetaData.entityNameToIndex(entityName)
    val indexBitmap = sortedValues.map(_._1).foldLeft(0){(acc, index) => acc | (1 << index)} 
	val svDataPoolIndexes = sortedValues.map(maybePool(entityId, _))
	
	val b = PBEntity.newBuilder().setEntityId(entityId).setBitmap(indexBitmap)
	svDataPoolIndexes.foreach(b.addPoolIndexes(_))
	
	val bytes = b.build.toByteArray()
	val cepb = new CEPB(bytes)
	
	sum = sum + bytes.length
	count = count + 1
	if (count % 100000 == 0) {
	  println("avg byte size = " + (sum/count).toInt)
	}
	
	cepb
  }
  
  private def maybePool(entityId: Int, columnIndexAndValue: (Int, Any)): Int = {
    if (CEPBMetaData.poolValue(entityId, columnIndexAndValue._1)) {
      DataPool.index(columnIndexAndValue._2)
    } else {
      columnIndexAndValue._2 match {
        case integer: Int => integer
        case _ => throw new Exception("At the moment it is only supported not to pool integers")
      }
    }
  }
  
  private def toEntity(pbeBytes: Array[Byte]) : Entity = {
    val pbe = PBEntity.parseFrom(pbeBytes)
    val entityName = CEPBMetaData.intToEntityName(pbe.getEntityId())
    val indexes = (0 to 31).foldRight(List[Int]()) {(index, indexList) => 
	    if (containsIndex(pbe, index)) {
	      index :: indexList
	    } else indexList
	}
    val entityId = pbe.getEntityId()
    
    val ColumnAndPoolIndexTuples =  indexes.zip(pbe.getPoolIndexesList())
    val values = ColumnAndPoolIndexTuples.map{case (columnIndex, poolIndex) => DataPool.indexToValue(poolIndex,CEPBMetaData.getType(entityId, columnIndex))}
    val e = Map[String, Any](indexes.map(CEPBMetaData.indexToColumn(entityId, _)).zip(values):_*)
    e
  }
  
  private def containsIndex(pbe: PBEntity, index: Int): Boolean = {
	  (pbe.getBitmap() & (1 << index)) > 0
  }
}

class CEPB(val pbeBytes: Array[Byte]) extends CompactEntity{
  def get() : Entity = CEPB.toEntity(pbeBytes)
  
  override def equals(o: Any): Boolean = {
    val anyRef = o.asInstanceOf[AnyRef]
	if (this eq anyRef) 
	  return true;
	if (o == null)
		return false;
	if (getClass() != o.getClass())
		return false;
	val other = o.asInstanceOf[CEPB];
	if (!Arrays.equals(pbeBytes, other.pbeBytes))
		return false;
	return true;
  }
  
  override def hashCode(): Int = {
	val prime = 31;
	prime + Arrays.hashCode(pbeBytes);
  }
}