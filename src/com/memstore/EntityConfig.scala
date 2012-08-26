package com.memstore

import com.memstore.Types.{Entity, Index}
import com.memstore.index.IndexImpl
import scala.collection.immutable.TreeMap
import com.memstore.index.DateIndex

class EntityConfig(val name: String, val key: String, val indexes: IndexConfig[_]*) {
  def this(name: String) = this(name, "id")

}

class IndexConfig[IndexType<%Ordered[IndexType]](val name: String, indexMethod: Entity => IndexType) {
  def emptyIndex: Index = {
     (new IndexImpl(TreeMap[IndexType, DateIndex](), indexMethod)).asInstanceOf[Index] //how should this be done the correct way 
    }
}