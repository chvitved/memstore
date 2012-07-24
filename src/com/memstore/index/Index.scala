package com.memstore.index
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity

object Index {
  
  def apply[IndexType<%Ordered[IndexType]](indexMethod: Entity => IndexType) = {
    new IndexImpl(TreeMap[IndexType, DateIndex](), indexMethod)
  }
}

class IndexImpl[IndexType<%Ordered[IndexType]] (map: SortedMap[IndexType, DateIndex], indexMethod: Entity => IndexType) {
  
  def === (key: IndexType) : DateIndex = {
    map.getOrElse(key, DateIndex())
  }
  
  def + (date: Date, et: EntityTimeline): IndexImpl[IndexType] = {
    val key = indexMethod(et.get(date))
    val newMap = if (key != null) {
    	val di = map.getOrElse(key, DateIndex()) + (date, et)
    	map + (key -> di)
    } else map //TODO we should probably not just throw away null values
    new IndexImpl(newMap, indexMethod)
  }
  
  def - (date: Date, et: EntityTimeline): IndexImpl[IndexType] = {
    val key = indexMethod(et.get(date))
    val di = map(key)  - (date, et)
    val newMap = map + (key -> di)
    new IndexImpl(newMap, indexMethod)
  }
  
}