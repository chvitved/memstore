package com.memstore.entity

import com.memstore.Types.{Entity, Index}
import scala.collection.immutable.TreeMap
import com.memstore.index.DateIndex

case class EntityConfig(name: String, key: String, val notPooledColumns: Seq[String], indexes: IndexConfig[_]*) {
  def this(name: String) = this(name, "id", Array[String]())
  def this(name: String, notPooledColumns: Seq[String]) = this(name, "id", notPooledColumns)

}

case class IndexConfig[IndexType<%Ordered[IndexType]](name: String, indexMethod: Entity => IndexType) {
  def emptyIndex: Index = {
     (new com.memstore.index.Index(TreeMap[IndexType, DateIndex](), indexMethod)).asInstanceOf[Index] //how should this be done the correct way 
    }
}