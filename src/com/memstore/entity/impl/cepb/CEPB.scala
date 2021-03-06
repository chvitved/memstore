package com.memstore.entity.impl.cepb

import scala.collection.JavaConversions._
import com.memstore.entity.CompactEntity
import com.memstore.Types.Entity
import com.memstore.serialization.Serialization.PBEntity
import java.util.Arrays
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.CompactEntityDataPool
import com.memstore.serialization.Serializer
import com.memstore.serialization.DeSerializer

object CEPB {
  
  var count: Int = 0
  var sum: Int = 0
  
  def apply(e: Entity, compactEntityMetaData: CompactEntityMetaData, dataPool: CompactEntityDataPool): (CEPB, CEPBMetaData, CEPBDataPool) = {
    val metaData = compactEntityMetaData.asInstanceOf[CEPBMetaData]
    val pool = dataPool.asInstanceOf[CEPBDataPool]
    val (newMetaData, indexValueTuples) = metaData.getIndexesAndUpdateMetaData(e)
    
    val sortedValues = indexValueTuples.toArray.sortWith{(v1, v2) => v1._1 < v2._1} 
    
    //pooled values
    val sortedPooledValues = sortedValues.filter(t => newMetaData.poolValue(t._1))
    val indexBitmap = sortedPooledValues.map(_._1).foldLeft(0){(acc, index) => acc | (1 << index)}
    val (newPool, svDataPoolIndexes) = sortedPooledValues.foldRight((pool, List[Int]())) {(value, tuple) =>
      val pool = tuple._1
      val indexList = tuple._2
      val (index, newPool) = pool.index(value._2)
      (newPool, index :: indexList)
    }
    val b = PBEntity.newBuilder().setBitmap(indexBitmap)
	svDataPoolIndexes.foreach(b.addPoolIndexes(_))
    
    //not pooled values
    val sortedNotPooledValues = sortedValues.filter(t => !newMetaData.poolValue(t._1))
    val notPooledBitmap = sortedNotPooledValues.map(_._1).foldLeft(0){(acc, index) => acc | (1 << index)}
    val notPooledValues = sortedNotPooledValues.map(_._2)
	
    b.setNotPooledBitmap(notPooledBitmap)
    notPooledValues.foreach(v => b.addNotPooledValue(Serializer.toPBValue(v)))
	
	val bytes = b.build.toByteArray()
	val cepb = new CEPB(bytes)
	
	sum = sum + bytes.length
	count = count + 1
	if (count % 100000 == 0) {
	  //println("avg byte size = " + (sum/count).toInt)
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
    
    val pooledIndexes = getIndexes(pbe.getBitmap())
    val columnAndPoolIndexTuples =  pooledIndexes.zip(pbe.getPoolIndexesList())
    val pooledValues = columnAndPoolIndexTuples.map{case (columnIndex, poolIndex) => getValueFromIndex(poolIndex, columnIndex, pool, metaData)}
    val keyValues = pooledIndexes.map(metaData.indexToColumn(_)).zip(pooledValues)
    
    val notPooledIndexes = getIndexes(pbe.getNotPooledBitmap())
    val keyValues1 = notPooledIndexes.map(metaData.indexToColumn(_)).zip(pbe.getNotPooledValueList().map(DeSerializer.toValue(_)))
    
    val e = Map[String, Any]((keyValues ++ keyValues1):_*)
    e
  }
  
  private def getValueFromIndex(poolIndex: Int, columnIndex: Int, pool: CEPBDataPool, metaData: CEPBMetaData) : Any = {
    val clas = metaData.getType(columnIndex)
    pool.indexToValue(poolIndex, clas)
  }
  
  private def getIndexes(bitmap: Int): List[Int] = {
    (0 to 31).foldRight(List[Int]()) {(index, indexList) => 
	    if (containsIndex(bitmap, index)) {
	      index :: indexList
	    } else indexList
	}
  }
  
  private def containsIndex(bitmap: Int, index: Int): Boolean = {
	  (bitmap & (1 << index)) > 0
  }
  
  def poolStats(ce: CompactEntity, compactEntityMetaData: CompactEntityMetaData, dataPool: CompactEntityDataPool): List[(String,Class[_], Int)] = {
    val pool = dataPool.asInstanceOf[CEPBDataPool]
    val metaData = compactEntityMetaData.asInstanceOf[CEPBMetaData]
    
    val pbe = PBEntity.parseFrom(ce.asInstanceOf[CEPB].pbeBytes)
    val pooledIndexes = getIndexes(pbe.getBitmap())
    val columnAndPoolIndexTuples =  pooledIndexes.zip(pbe.getPoolIndexesList())
    
    columnAndPoolIndexTuples.foldLeft(List[(String,Class[_], Int)]()) {(list, columnIndexAndPoolIndex) =>
      val columnName = metaData.indexToColumn(columnIndexAndPoolIndex._1)
      val clas = metaData.getType(columnIndexAndPoolIndex._1)
      val index: Int = columnIndexAndPoolIndex._2
      (columnName, clas, index) :: list
    }
    
    
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