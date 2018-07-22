package com.memstore.index.clustered
import com.memstore.index.Index
import java.util.ArrayList
import java.io.InputStream
import java.nio.ByteBuffer
import scala.math.Ordering
import java.io.ByteArrayInputStream

object NewClusteredIndex {
	val clusterSize = 16
	
	def empty[A: ClassManifest,B](ordering: Ordering[A], 
      emptyValue: => B, 
      valueFromBytes: (InputStream) => B,
      valueToBytes: B => Array[Byte]): NewClusteredIndex[A, B] = {
    		  new NewClusteredIndex(ClusteredMap.empty(ordering), ordering, emptyValue, valueFromBytes, valueToBytes)
  }
	
}
/**
 * todo first simple implementation
 * Later keep the arrays sorted so they can be binary searched
 */
class NewClusteredIndex[A: ClassManifest, B] private (val map: ClusteredMap[A, Node[A]], implicit val ordering: Ordering[A],
    emptyValue: => B, 
    valueFromBytes: (InputStream) => B,
    valueToBytes: B => Array[Byte]
	) extends Index[A,B] {
  
  override def empty = emptyValue
  
  def make(map: ClusteredMap[A, Node[A]]): NewClusteredIndex[A, B] = {
    new NewClusteredIndex(map, ordering, emptyValue, valueFromBytes, valueToBytes)
  }
  
  override def + (kv: (A, B)): Index[A, B] = {
     val (key, value) = kv
     map.floor(key) match {
       case None => {
         val keys = new Array[A](NewClusteredIndex.clusterSize)
         keys(0) = key
         val elements = Array.ofDim[Array[Byte]](NewClusteredIndex.clusterSize)
         elements(0) = valueToBytes(value)
         make(map + (key -> new Node(keys, elements, 1)))
       }
       case Some((existingKey, node)) => {
         val index = node.indexOfKey(key) 
         if (index >= 0) { //does the key already exist
           val newNode = node.copy
           newNode.elements(index) = valueToBytes(value)
           make(map + (existingKey -> newNode))
         } else {
           val size = node.size
           if (size < NewClusteredIndex.clusterSize) {
             val newNode = node.copy
             newNode.keys(size) = key
             newNode.elements(size) = valueToBytes(value)
             newNode.size = size + 1
             make(map + (existingKey -> newNode))
           } else {
             //split the node
             val zipped=  node.keys.zip(node.elements)
             val sortedZipped = zipped.sortWith((t1, t2) => ordering.lt(t1._1, t2._1))
             val (a1, a2) = sortedZipped.splitAt(NewClusteredIndex.clusterSize/2)
             val key2 = a2(0)._1
             
             //TODO below we are going back and forth between arrays and lists - that is expensive
             val (newL1, newL2) = 	if (ordering.lt(key, key2)) ((key -> valueToBytes(value)) +: a1.toList, a2.toList)
  									else (a1.toList, (key -> valueToBytes(value)) +: a2.toList)
  			val (keys1, elements1) = newL1.unzip
  			val (keys2, elements2) = newL2.unzip
  			
  			val node1 = new Node(keys1.toArray, elements1.toArray, keys1.size)
            val node2 = new Node(keys2.toArray, elements2.toArray, keys2.size)
  			
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
    map.values.foldLeft(Set[A]()) {(set, node) =>
    	set ++ node.keys.take(node.size)
    }
  }
  
  def values: Iterable[B] = map.values.flatMap{node => node.elements.take(node.size).map(bytes => value(bytes))}
  
  def elements : Iterable[(A, B)] = map.values.flatMap(node => node.keys.take(node.size)zip(node.elements.take(node.size).map(bytes => value(bytes))))
  
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

class Node[A: ClassManifest](val keys: Array[A], val elements: Array[Array[Byte]], var size: Int) {
  def copy: Node[A] = {
    val newKeys = new Array[A](NewClusteredIndex.clusterSize)
    val newElements = Array.ofDim[Array[Byte]](NewClusteredIndex.clusterSize)
    Array.copy(keys, 0, newKeys, 0, size)
    Array.copy(elements, 0, newElements, 0, size)
    new Node(newKeys, newElements, size)
  }
  
  def indexOfKey(key: A): Int = keys.indexWhere(_ == key) 
}
//    if (buffer.remaining() <= bytes.size) {
//      currentIndex = currentIndex + 1
//      buffer = indexer.get(currentIndex)
//      indexer.add(buffer)
//    }
//    val offset = buffer.position()
//    buffer.put(bytes)
//    (currentIndex,offset)
//  }
//}