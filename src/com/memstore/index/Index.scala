package com.memstore.index
import com.memstore.entity.EntityTimeline
import scala.collection.Iterator

trait Index[A,B] {

  def apply(key: A): B
  
  def empty: B
  
  def + (kv: (A, B)): Index[A, B]
  
  def keys: Iterable[A]
  
  def values: Iterable[B]
  
  def elements : Iterable[(A, B)]
  
  def size: Int
  
  def from(key: A): Index[A,B]
  
  def until(key: A): Index[A,B]
  
  def range(from: A, until: A): Index[A,B]
}