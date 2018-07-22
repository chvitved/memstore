package com.memstore.index.clustered
import com.memstore.index.Index

class IndexwithFilter[A,B](i: Index[A,B], private val filter: A => Boolean) extends Index[A,B] {

  def apply(key: A): B = if (filter(key)) i(key) else i.empty 
  
  def + (kv: (A, B)): Index[A, B] = throw new Exception("Adding to a filtered view of an index")
  
  def values: Iterable[B] = i.elements.view.collect{case t if (filter(t._1)) => t._2}
  
  def elements : Iterable[(A, B)] = i.elements.view.filter(t => filter(t._1))
  
  def size: Int = i.keys.foldLeft(0) {(sum, key) => if (filter(key)) sum + 1 else sum}
  
  def from(key: A): Index[A,B] = new IndexwithFilter(i.from(key), filter)
  
  def until(key: A): Index[A,B] = new IndexwithFilter(i.until(key), filter)
  
  def range(from: A, until: A): Index[A,B] = new IndexwithFilter(i.range(from, until), filter)
  
  def keys: Iterable[A] = i.keys.view.filter(filter(_))
  
  def empty: B = i.empty
}