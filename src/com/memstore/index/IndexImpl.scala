package com.memstore.index
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity

object IndexImpl {
  
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
	val prevEntity = et.get(new Date(date.getTime() - 1)) //get the value before the current insert
    val prviousKey = if (prevEntity == null) null else indexMethod(prevEntity) 
    if (key != prviousKey) {
    	if (key != null) {
    		val di = map.getOrElse(key, DateIndex()) + (date, et)
    		val newMap = map + (key -> di)
    		new IndexImpl(newMap, indexMethod)
    	} else {
	      this - (date, et) // is null the same as removing an element?
	    }
    } else this
    
  }
  
  def - (date: Date, et: EntityTimeline): IndexImpl[IndexType] = {
    //key is previous value in index
    val key = indexMethod(et.get(new Date(date.getTime() - 1)))
    val di = map(key)  - (date, et)
    val newMap = map + (key -> di)
    new IndexImpl(newMap, indexMethod)
  }
  
}