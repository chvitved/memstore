package com.memstore

import com.memstore.Types.{Entity, Index}
import scala.collection.immutable.TreeMap
import com.memstore.index.DateIndex

class EntityConfig(val name: String, val key: String/*, val notPooledColumns: Seq[String]*/, val indexes: IndexConfig[_]*) {
  def this(name: String) = this(name, "id"/*, Array[String]()*/)

}

class IndexConfig[IndexType<%Ordered[IndexType]](val name: String, indexMethod: Entity => IndexType) {
  def emptyIndex: Index = {
     (new com.memstore.index.Index(TreeMap[IndexType, DateIndex](), indexMethod)).asInstanceOf[Index] //how should this be done the correct way 
    }
}