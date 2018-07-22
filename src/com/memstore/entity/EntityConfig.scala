package com.memstore.entity

import com.memstore.Types.{Entity, SecondaryIndex}
import scala.collection.immutable.TreeMap
import com.memstore.index.DateIndex
import com.memstore.index.Index
import com.memstore.index.factory.PrimaryIndexFactory

object EntityConfig{
  def apply(name: String): EntityConfig = EntityConfig(name, "id", PrimaryIndexFactory(), Array[String](), List())
  def apply(name: String, key: String): EntityConfig = EntityConfig(name, key, PrimaryIndexFactory(), Array[String](), List())
  def apply(name: String, notPooledColumns: Seq[String], indexes: Seq[String]) : EntityConfig = EntityConfig(name, "id", PrimaryIndexFactory(), notPooledColumns, indexes) 
  def apply(name: String, indexes: String*) : EntityConfig = EntityConfig(name, "id", PrimaryIndexFactory(), List(), indexes)
  def apply(name: String, key: String, notPooledColumns: Seq[String], indexes: Seq[String]) : EntityConfig = EntityConfig(name, key, PrimaryIndexFactory(), notPooledColumns, indexes)
}

case class EntityConfig(name: String, key: String, primaryIndex: Index[Any, EntityTimeline], val notPooledColumns: Seq[String], indexes: Seq[String])