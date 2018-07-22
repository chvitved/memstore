package com.memstore.index

trait EmptyIndex[A,B] extends Index[A,B]{
  
  def create(key: A): Index[A,B]
  def empty : B

  def apply(key: A): B = empty
  
  def + (kv: (A, B)): Index[A, B] = create(kv._1) + (kv)
  
  def values: Iterable[B] = Iterable.empty
  
  def elements : Iterable[(A, B)] = Iterable.empty 
  
  def size: Int = 0
  
  def from(key: A): Index[A,B] = this
  
  def until(key: A): Index[A,B] = this
  
  def range(from: A, until: A): Index[A,B] = this
  
  def keys: Iterable[A] = Iterable.empty
}
