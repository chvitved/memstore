package com.memstore.index.clustered
import com.memstore.index.Index
import java.util.ArrayList
import java.io.InputStream
import java.nio.ByteBuffer
import scala.math.Ordering
import java.io.ByteArrayInputStream
import com.memstore.serialization.Serialization.PBNodeKeys
import com.memstore.serialization.Serializer
import com.memstore.serialization.DeSerializer
import scala.collection.JavaConversions._
import com.memstore.serialization.Serialization.PBValue

object New1ClusteredIndex {
	val clusterSize = 16
	
	def empty[A,B](ordering: Ordering[A], 
      emptyValue: => B, 
      valueFromBytes: (InputStream) => B,
      valueToBytes: B => Array[Byte]): New1ClusteredIndex[A, B] = {
    		  new New1ClusteredIndex(ClusteredMap.empty(ordering), ordering, emptyValue, valueFromBytes, valueToBytes)
  }
	
}
/**
 * todo first simple implementation
 * Later keep the arrays sorted so they can be binary searched
 */
class New1ClusteredIndex[A, B] private (val map: ClusteredMap[A, Node1[A]], implicit val ordering: Ordering[A],
    emptyValue: => B, 
    valueFromBytes: (InputStream) => B,
    valueToBytes: B => Array[Byte]
	) extends Index[A,B] {
  
  override def empty = emptyValue
  
  def make(map: ClusteredMap[A, Node1[A]]): New1ClusteredIndex[A, B] = {
    new New1ClusteredIndex(map, ordering, emptyValue, valueFromBytes, valueToBytes)
  }
  
  override def + (kv: (A, B)): Index[A, B] = {
     val (key, value) = kv
     map.floor(key) match {
       case None => make(map + (key -> Node1.create(key, valueToBytes(value))))
       case Some((existingKey, node)) => {
         val index = node.indexOfKey(key) 
         if (index >= 0) { //does the key already exist
           val newNode = node.update(index, key, valueToBytes(value))
           make(map + (existingKey -> newNode))
         } else {
           if (node.size < New1ClusteredIndex.clusterSize) {
             val newNode = node.add(key, valueToBytes(value))
             make(map + (existingKey -> newNode))
           } else {
             //split the Node
             val zipped=  node.keys.zip(node.elements)
             val sortedZipped = zipped.sortWith((t1, t2) => ordering.lt(t1._1, t2._1))
             val (a1, a2) = sortedZipped.splitAt(New1ClusteredIndex.clusterSize/2)
             val key2 = a2(0)._1
             
             //TODO below we are going back and forth between arrays and lists - that is expensive
             val (newL1, newL2) = 	if (ordering.lt(key, key2)) ((key -> valueToBytes(value)) +: a1.toList, a2.toList)
  									else (a1.toList, (key -> valueToBytes(value)) +: a2.toList)
  			val (keys1, elements1) = newL1.unzip
  			val (keys2, elements2) = newL2.unzip
  			
  			val node1 = Node1.create(keys1, elements1)
            val node2 = Node1.create(keys2, elements2)
  			
  			val m1 = map + (existingKey -> node1)
  			val m2 = m1 + (key2 -> node2)
  			make(m2)
         }
       }
     }
    }
  }
  
  override def apply(key: A): B = {
    map.floor(key) match {
      case None => emptyValue
      case Some((k, node)) => {
        val index = node.indexOfKey(key)
        if (index >= 0) value(node.elements(index))
        else emptyValue
      }
    }
  }
  
  private def value(bytes: Array[Byte]): B = valueFromBytes(new ByteArrayInputStream(bytes))
  
  def keys : Iterable[A] = {
    map.values.foldLeft(Set[A]()) {(set, Node1) =>
    	set ++ Node1.keys.take(Node1.size)
    }
  }
  
  def values: Iterable[B] = map.values.flatMap{Node1 => Node1.elements.take(Node1.size).map(bytes => value(bytes))}
  
  def elements : Iterable[(A, B)] = map.values.flatMap(Node1 => Node1.keys.take(Node1.size)zip(Node1.elements.take(Node1.size).map(bytes => value(bytes))))
  
  def size: Int = keys.size
  
  def from(key: A): Index[A, B] = { //inclusive
    new IndexwithFilter(make(map.from(key)), (k: A) => ordering.gteq(k, key))
  }
  
  def until(key: A): Index[A, B] = { //exclusive
    new IndexwithFilter(make(map.until(key)), (k: A) => ordering.lt(k, key))
  }
  
  def range(from: A, until: A): Index[A, B] = {
    new IndexwithFilter(make(map.range(from, until)), (k: A) => (ordering.lt(k, until) && ordering.gteq(k, from)))
  }
  
}

object Node1{
	
	def create[A](key: A, value: Array[Byte]): Node1[A] = new Node1(PBNodeKeys.newBuilder().addKey(toPbKey(key)).build.toByteArray, Array[Array[Byte]](value), 1)
	
	def create[A](keys: Seq[A], values: Seq[Array[Byte]]): Node1[A] = {
	  val keyBytes = PBNodeKeys.newBuilder().addAllKey(keys.map(toPbKey(_))).build.toByteArray
	  val elements = Array[Array[Byte]](values:_*)
	  new Node1(keyBytes, elements, elements.size)
	}
	
	private def toPbKey[A](key: A): PBValue = {
		Serializer.toPBValue(key)
	}
}

class Node1[A](val keyBytes: Array[Byte], val elements: Array[Array[Byte]], var size: Int) {
  
  def update(index: Int, key: A, value: Array[Byte]): Node1[A] = {
    val pbKeys = PBNodeKeys.parseFrom(keyBytes)
    val newKeyBytes = pbKeys.toBuilder().setKey(index, Node1.toPbKey(key)).build().toByteArray()
    val newElements = copyElementsArray(size)
    newElements(index) = value
    new Node1(newKeyBytes, newElements, size)
  }
  
  def add(key: A, value: Array[Byte]): Node1[A] = {
    val pbKeys = PBNodeKeys.parseFrom(keyBytes)
    val newKeys = pbKeys.toBuilder().addKey(Node1.toPbKey(key)).build
    val newElements = copyElementsArray(size + 1)
    newElements(size) = value
    new Node1(newKeys.toByteArray(), newElements, size + 1)
  }
  
  private def copyElementsArray(newSize: Int): Array[Array[Byte]] = {
    val newElements = new Array[Array[Byte]](newSize)
    Array.copy(elements, 0, newElements, 0, size)
    newElements
  }
  
  def keys: Seq[A] = PBNodeKeys.parseFrom(keyBytes).getKeyList().map(DeSerializer.toValue(_).asInstanceOf[A])
  
  def indexOfKey(key: A): Int = keys.indexWhere(_ == key) 
}
