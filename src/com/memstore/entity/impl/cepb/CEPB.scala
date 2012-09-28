package com.memstore.entity.impl.cepb

import scala.collection.JavaConversions._
import com.memstore.entity.CompactEntity
import com.memstore.Types.Entity
import com.memstore.serialization.Serialization.PBEntity
import java.util.Arrays
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.CompactEntityDataPool

object CEPB {
  
  var count: Int = 0
  var sum: Int = 0
  
  def apply(e: Entity, compactEntityMetaData: CompactEntityMetaData, dataPool: CompactEntityDataPool): (CEPB, CEPBMetaData, CEPBDataPool) = {
    val metaData = compactEntityMetaData.asInstanceOf[CEPBMetaData]
    val pool = dataPool.asInstanceOf[CEPBDataPool]
    val (newMetaData, indexValueTuples) = metaData.getIndexesAndUpdateMetaData(e)
    
    val sortedValues = indexValueTuples.toArray.sortWith{(v1, v2) => v1._1 < v2._1} 
    
    val indexBitmap = sortedValues.map(_._1).foldLeft(0){(acc, index) => acc | (1 << index)}
    
    val (newPool, svDataPoolIndexes) = sortedValues.foldRight((pool, List[Int]())) {(value, tuple) =>
      val pool = tuple._1
      val indexList = tuple._2
      val (index, newPool) = maybePool(value, pool, metaData)
      (newPool, index :: indexList)
    }
	
	val b = PBEntity.newBuilder().setBitmap(indexBitmap)
	svDataPoolIndexes.foreach(b.addPoolIndexes(_))
	val bytes = b.build.toByteArray()
	val cepb = new CEPB(bytes)
	
	sum = sum + bytes.length
	count = count + 1
	if (count % 100000 == 0) {
	  println("avg byte size = " + (sum/count).toInt)
	}
	(cepb, newMetaData, newPool)
  }
  
  private def maybePool(columnIndexAndValue: (Int, Any), pool: CEPBDataPool, metaData: CEPBMetaData): (Int, CEPBDataPool) = {
    if (metaData.poolValue(columnIndexAndValue._1)) {
      pool.index(columnIndexAndValue._2)
    } else {
      columnIndexAndValue._2 match {
        case integer: Int => (integer, pool)
        case _ => throw new Exception("At the moment it is only supported not to pool integers")
      }
    }
  }
  
  private def toEntity(pbeBytes: Array[Byte], compactEntityMetaData: CompactEntityMetaData, dataPool: CompactEntityDataPool) : Entity = {
    val pool = dataPool.asInstanceOf[CEPBDataPool]
    val metaData = compactEntityMetaData.asInstanceOf[CEPBMetaData]
    val pbe = PBEntity.parseFrom(pbeBytes)
    val indexes = (0 to 31).foldRight(List[Int]()) {(index, indexList) => 
	    if (containsIndex(pbe, index)) {
	      index :: indexList
	    } else indexList
	}
    
    val columnAndPoolIndexTuples =  indexes.zip(pbe.getPoolIndexesList())
    val values = columnAndPoolIndexTuples.map{case (columnIndex, poolIndex) => pool.indexToValue(poolIndex, metaData.getType(columnIndex))}
    val e = Map[String, Any](indexes.map(metaData.indexToColumn(_)).zip(values):_*)
    e
  }
  
  private def containsIndex(pbe: PBEntity, index: Int): Boolean = {
	  (pbe.getBitmap() & (1 << index)) > 0
  }
}

class CEPB(val pbeBytes: Array[Byte]) extends CompactEntity{
  override def get(metaData: CompactEntityMetaData, pool: CompactEntityDataPool) : Entity = CEPB.toEntity(pbeBytes, metaData, pool)
  
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