package com.memstore.index
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity
import com.memstore.Types.EntityTimelineWithId

object IndexImpl {
  
  def apply[IndexType<%Ordered[IndexType]](indexMethod: Entity => IndexType) = {
    new IndexImpl(TreeMap[IndexType, DateIndex](), indexMethod)
  }
}

class IndexImpl[IndexType<%Ordered[IndexType]] (map: SortedMap[IndexType, DateIndex], indexMethod: Entity => IndexType) {
  
  def === (key: IndexType) : DateIndex = {
    map.getOrElse(key, DateIndex())
  }
  
  def + (date: Date, et: EntityTimelineWithId): IndexImpl[IndexType] = {
    et.et.get(date) match {
      case None => throw new Exception("there should be an entity when adding one")
      case Some(e: Entity) => {
    	val key = indexMethod(e)
    	val prevEntity = et.et.get(new Date(date.getTime() - 1)) //get the value before the current insert
    	val changed =  prevEntity match {
    	  case None => true
    	  case Some(e) => indexMethod(e) != key
    	}
	    if (changed) {
	    	if (key != null) { //how should we handle null
	    		val di = map.getOrElse(key, DateIndex()) + (date, et)
	    		val newMap = map + (key -> di)
	    		new IndexImpl(newMap, indexMethod)
	    	} else {
		      this - (date, et) // is null the same as removing an element?
		    }
	    } else this
      }
    }
  }
  
  def - (date: Date, et: EntityTimelineWithId): IndexImpl[IndexType] = {
    //key is previous value in index
    et.et.get(new Date(date.getTime() - 1)) match {
      case None => throw new Exception("entity is already deleted") // should we not throw an exception
      case Some(e) => {
    	val key = indexMethod(e)
	    val di = map(key)  - (date, et)
	    val newMap = map + (key -> di)
	    new IndexImpl(newMap, indexMethod)
      }
    }
  }
  
  def range(date: Date, from: IndexType, until: IndexType) : Set[Entity] = {
    val dateIndexes = map.range(from, until).values
    dateIndexes.foldLeft(Set[Entity]()){(set, di) =>
      set ++ di.get(date)
    }
  }
  
}