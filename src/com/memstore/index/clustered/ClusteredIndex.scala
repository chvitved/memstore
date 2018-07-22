package com.memstore.index.clustered

import scala.collection.JavaConversions._
import com.memstore.entity.impl.cepb.CEPB
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.CompactEntityDataPool
import com.memstore.serialization.DeSerializer
import com.memstore.serialization.Serialization.PBValue
import java.util.Arrays
import java.io.InputStream
import java.io.ByteArrayInputStream
import com.memstore.serialization.Serialization.Keys
import com.memstore.serialization.Serialization.Key
import java.io.ByteArrayOutputStream
import com.memstore.serialization.Serializer
import com.google.protobuf.CodedInputStream
import com.google.protobuf.CodedOutputStream
import com.memstore.index.Index
import scala.math.Ordering
import scala.collection.immutable.TreeSet
import scala.collection.immutable.RedBlack
import com.memstore.util.TimeUtil
import scala.collection.mutable.Buffer

object ClusteredIndex {
  
  val clusterSize = 16
  
  val bytesForKeyIndex = 4
  
  def empty[A,B](ordering: Ordering[A], 
      emptyValue: => B, 
      valueFromBytes: (InputStream) => B,
      valueToBytes: B => Array[Byte]): ClusteredIndex[A, B] = {
    		  new ClusteredIndex(ClusteredMap.empty(ordering), ordering, emptyValue, valueFromBytes, valueToBytes)
  }
}

class ClusteredIndex[A, B] (val map: ClusteredMap[A, Array[Byte]], implicit val ordering: Ordering[A], 
    emptyValue: => B, 
    valueFromBytes: (InputStream) => B,
    valueToBytes: B => Array[Byte])extends Index[A,B]{
  
  def empty = emptyValue
  
  def make(map: ClusteredMap[A, Array[Byte]]): ClusteredIndex[A, B] = {
    new ClusteredIndex(map, ordering, emptyValue, valueFromBytes, valueToBytes)
  }
  
  def from(key: A): Index[A, B] = { //inclusive
    new IndexwithFilter(make(map.from(key)), (k: A) => ordering.gteq(k, key))
  }
  
  def until(key: A): Index[A, B] = { //exclusive
    new IndexwithFilter(make(map.until(key)), (k: A) => ordering.lt(k, key))
  }
  
  def range(from: A, until: A): Index[A, B] = {
    new IndexwithFilter(make(map.range(from, until)), (k: A) => (ordering.lt(k, until) && ordering.gteq(k, from)))
  }
  
  def size: Int = map.values.foldLeft(0) {(sum, bytes) =>
    sum + keysFromBytes(bytes).getKeyCount()
  }
  
  def keys : Iterable[A] = {
    map.values.foldLeft(Set[A]()) {(set, bytes) =>
    	set ++ keysFromBytes(bytes).getKeyList().map{key => valueOf(key.getKey())}
    }
  }
  
  override def elements: Iterable[(A, B)] = {
    map.values.flatMap{bytes =>
      val keys = keysFromBytes(bytes)
      val values = valuesFromBytes(keys, bytes)
      deSerializeKeys(keys).zip(values)
    }
  } 
  
  override def values: Iterable[B] = {
    map.values.flatMap{bytes => valuesFromBytes(keysFromBytes(bytes), bytes)}
  }
  
  override def apply(key: A): B = {
    map.floor(key) match {
      case None => emptyValue
      case Some((k, bytes)) => {
        val keys = keysFromBytes(bytes)
        findKey(key, keys) match {
          case Some((index, key)) => valueFromKey(bytes, key)
          case None => emptyValue
        } 
      }
    }
  }
  
  private def findKey(key: A, keysWithPointers: Keys): Option[(Int, Key)] = {
    val keys = keysWithPointers.getKeyList()
    val index = keys.indexWhere(k => valueOf(k.getKey()) == key)
    if (index >= 0) Some((index, keys.get(index))) 
    else None
  }
  
  def valueFromKey(bytes: Array[Byte], k: Key): B = {
    valueFromBytes(bytes, k.getIndex(), k.getLength())
  }
  
  def valueFromBytes(bytes: Array[Byte], start: Int, length: Int): B = {
    valueFromBytes(new ByteArrayInputStream(bytes, start, length))
  }
  
  private def getKeyIndex(bytes: Array[Byte]): Int = {
    CodedInputStream.newInstance(bytes).readFixed32()
  }
  
  private def keysFromBytes(bytes: Array[Byte]) : Keys = {
    keysFromBytes(bytes, getKeyIndex(bytes))
  }
  
  private def keysFromBytes(bytes: Array[Byte], keyIndex: Int): Keys = {
    Keys.parseFrom(new ByteArrayInputStream(bytes, keyIndex, bytes.length - keyIndex))
  }
  
  private def valuesFromBytes(keys: Keys, bytes: Array[Byte]) : Seq[B] = {
    keys.getKeyList().map{k => valueFromKey(bytes, k)}
  }
  
  private def toBytes(elements: Seq[(A,B)]) : Array[Byte] = {
    val keysWithValueBytes = elements.map(t => (t._1, valueToBytes(t._2)))
    val valueArraySize = keysWithValueBytes.foldLeft(0) {(sum,t) => sum + t._2.length}
    
    val keys = Keys.newBuilder()
    var index = ClusteredIndex.bytesForKeyIndex
    for(t <- keysWithValueBytes) {
      val length = t._2.size
      val key = Key.newBuilder().setKey(Serializer.toPBValue(t._1)).setIndex(index).setLength(length).build
      keys.addKey(key)
      index += length
    }
    val newKeys = keys.build

    val resultingArray = new Array[Byte](ClusteredIndex.bytesForKeyIndex + valueArraySize + newKeys.getSerializedSize())
    val cos = CodedOutputStream.newInstance(resultingArray)
    cos.writeFixed32NoTag(ClusteredIndex.bytesForKeyIndex + valueArraySize)
    for(t <- keysWithValueBytes)
    	cos.writeRawBytes(t._2)
    newKeys.writeTo(cos)    
    resultingArray
  }    
  
  private def addKey(keys: Keys.Builder, key: A, start: Int, length: Int): Keys = {
    keys.addKey(Key.newBuilder().setKey(Serializer.toPBValue(key)).setIndex(start).setLength(length))
    keys.build()
  }
    
  private def add(key: A, value: B, bytes: Array[Byte], keys: Keys, keyIndex: Int) : Array[Byte] = {
    val indexForNextValue = keys.getKeyList().foldLeft(ClusteredIndex.bytesForKeyIndex) {(index, key) =>
      val nextIndex = key.getIndex() + key.getLength()
      if (nextIndex > index) nextIndex
      else index
    }
    val valueBytes = valueToBytes(value)
    val newKeys = addKey(keys.toBuilder(), key, indexForNextValue, valueBytes.size)
    val resultingArray = new Array[Byte](keyIndex + newKeys.getSerializedSize() + valueBytes.size) 
    val newKeyIndex = keyIndex + valueBytes.size
    val cos = CodedOutputStream.newInstance(resultingArray)
    cos.writeFixed32NoTag(newKeyIndex)
    if (bytes.size > 0)
    	cos.writeRawBytes(bytes, ClusteredIndex.bytesForKeyIndex, keyIndex - ClusteredIndex.bytesForKeyIndex)
    cos.writeRawBytes(valueBytes)
    newKeys.writeTo(cos)
    resultingArray
  }
  
  private def updateExistingKeyValue(key: Key, updatedKeyIndex: Int, value: B, keys: Keys, bytes: Array[Byte]) : Array[Byte] = {
    val valueBytes = valueToBytes(value)
    val valueOffset = valueBytes.size - key.getLength() //new length - old length
    
    //update keys
    val newKeysBuilder = keys.toBuilder()
    val updatedKey = keys.getKey(updatedKeyIndex).toBuilder().setLength(valueBytes.size)
    newKeysBuilder.setKey(updatedKeyIndex, updatedKey)
    for(i <- (updatedKeyIndex +1 until keys.getKeyCount())) {
      val key = newKeysBuilder.getKey(i)
      val newKey = key.toBuilder().setIndex(key.getIndex() + valueOffset)
      newKeysBuilder.setKey(i, newKey)
    }
    val newKeys = newKeysBuilder.build()
    
    val keysIndexInOldByteArray = getKeyIndex(bytes)
    val resultingArray = new Array[Byte](keysIndexInOldByteArray + valueOffset + newKeys.getSerializedSize())
    val cos = CodedOutputStream.newInstance(resultingArray)
    cos.writeFixed32NoTag(getKeyIndex(bytes) + valueOffset)
    cos.writeRawBytes(bytes, ClusteredIndex.bytesForKeyIndex, key.getIndex() - ClusteredIndex.bytesForKeyIndex)
    cos.writeRawBytes(valueBytes)
    val indexAfterUpdatedValue = key.getIndex() + key.getLength()
    cos.writeRawBytes(bytes, indexAfterUpdatedValue, keysIndexInOldByteArray - indexAfterUpdatedValue)
    newKeys.writeTo(cos)
    resultingArray
  }
  
  private def deSerializeKeys(keys: Keys) : Seq[A] = {
    keys.getKeyList().map(k => valueOf(k.getKey()))
  }
  
  private def valueOf(key: PBValue) : A = {
    DeSerializer.toValue(key).asInstanceOf[A]
  }
  
  private def splitAndAdd(key: A, value: B, keys: Keys, bytes: Array[Byte]): (Seq[(A,B)], Seq[(A,B)], A) = {
    val values = valuesFromBytes(keys, bytes)
    val keysAndValuesSorted = deSerializeKeys(keys).zip(values).sortWith((a,b) => ordering.lt(a._1, b._1))
  
    val (list1, list2) = keysAndValuesSorted.splitAt(ClusteredIndex.clusterSize/2)
    val key2 = list2.head._1
    val (l1, l2) = 	if (ordering.lt(key, key2)) ((key -> value) +: list1, list2)
  							else (list1, (key -> value) +: list2)
  	(l1, l2, key2)
  }

  /**
   * TODO we need to be able to do some balancing of how many elements are in each node
   */
  def + (kv: (A, B)): ClusteredIndex[A,B] = {
    val (key, value) = kv
    map.floor(key) match {
	    case None => {
	      val newKeyBytes = add(key, value, Array(), Keys.newBuilder().build, ClusteredIndex.bytesForKeyIndex)
	      make(map + (key -> newKeyBytes))
	    }
	    case Some((existingKey, bytes)) => {
	      val keys = keysFromBytes(bytes)
		  findKey(key, keys) match {
		  	case Some((keyIndex, key)) => { // the key already exists and should be updated
		      val newBytes = updateExistingKeyValue(key, keyIndex, value, keys, bytes)
		      make(map + (existingKey -> newBytes))
	        }
		  	case None => {
		  	  if (keys.getKeyCount() < ClusteredIndex.clusterSize) {
		  		  val keyIndex = getKeyIndex(bytes)
			      val newBytes  = add(key, value, bytes, keys, keyIndex)
			      make(map + (existingKey -> newBytes))
		  	  }	else {
			      val (newL1, newL2, key2) = splitAndAdd(key, value, keys, bytes)
			      						
			      val map1 = map - existingKey
			      val map2 = map1 + (existingKey -> toBytes(newL1))
			      val map3 = map2 + (key2 -> toBytes(newL2))
			      
			      make(map3)
		  	  }
		  	}
	     }
	  }
    }
  }
  
  override def equals(o: Any) : Boolean = {
    val obj = o.asInstanceOf[AnyRef]
    if (this eq obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	val other: ClusteredIndex[_,_] = obj.asInstanceOf[ClusteredIndex[_,_]];
	if (!(ordering == other.ordering))
		return false;
	if (elements != other.elements)
		return false;
	return true;
  }
  
  override def hashCode: Int = {
	var prime = 31
	var result = 1
	result = prime * result + ordering.hashCode()
	result = prime * result + elements.hashCode()
	result
  }
  
}