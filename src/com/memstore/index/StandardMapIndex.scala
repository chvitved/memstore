package com.memstore.index
import com.memstore.entity.EntityTimeline
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.math.Ordering

object StandardMapIndex {
  def apply[A,B](emptyValue: => B)(implicit ordering: Ordering[A]) : StandardMapIndex[A,B] = new StandardMapIndex(TreeMap[A,B](), emptyValue)
}

class StandardPrimaryIndex[A](implicit ordering: Ordering[A]) extends StandardMapIndex[A, EntityTimeline](TreeMap[A,EntityTimeline](), EntityTimeline()) 

class StandardMapIndex[A,B](map: SortedMap[A, B], emptyValue: => B) extends Index[A, B]{
  
  def apply(key: A): B = map.getOrElse(key, emptyValue)
  
  def + (kv: (A, B)): Index[A, B] = new StandardMapIndex(map + kv, emptyValue) 
  
  def values: Iterable[B] = map.values
  
  def elements : Iterable[(A, B)] = map.iterator.toIterable
  
  def size: Int = map.size
  
  def empty: B = emptyValue
  
  def keys: Iterable[A] = map.keys
  
  def from(key: A): Index[A,B] = new StandardMapIndex(map.from(key), emptyValue)
  
  def until(key: A): Index[A,B] = new StandardMapIndex(map.until(key), emptyValue)
  
  def range(from: A, until: A): Index[A,B] = new StandardMapIndex(map.range(from, until), emptyValue)
}