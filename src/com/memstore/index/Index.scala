package com.memstore.index
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity
import com.memstore.Types.EntityTimelineWithId
import com.memstore.entity.CompactEntityMetaData

object Index {
  
  def apply[IndexType<%Ordered[IndexType]](indexMethod: Entity => IndexType) = {
    new Index(TreeMap[IndexType, DateIndex](), indexMethod)
  }
}

class Index[IndexType<%Ordered[IndexType]] (map: SortedMap[IndexType, DateIndex], indexMethod: Entity => IndexType) {
  
  def + (date: Date, et: EntityTimelineWithId, metaData: CompactEntityMetaData): Index[IndexType] = {
    et.et.get(date, metaData) match {
      case None => throw new Exception("there should be an entity when adding one")
      case Some(e: Entity) => {
    	val key = indexMethod(e)
    	val prevEntity = et.et.get(new Date(date.getTime() - 1), metaData) //get the value before the current insert
    	val changed =  prevEntity match {
    	  case None => true
    	  case Some(e) => indexMethod(e) != key
    	}
	    if (changed) {
	    	if (key != null) { //how should we handle null
	    		val di = map.getOrElse(key, DateIndex()) + (date, et)
	    		val newMap = map + (key -> di)
	    		new Index(newMap, indexMethod)
	    	} else {
		      this - (date, et, metaData) // is null the same as removing an element?
		    }
	    } else this
      }
    }
  }
  
  def - (date: Date, et: EntityTimelineWithId, metaData: CompactEntityMetaData): Index[IndexType] = {
    //key is previous value in index
    et.et.get(new Date(date.getTime() - 1), metaData) match {
      case None => throw new Exception("entity is already deleted") // should we not throw an exception
      case Some(e) => {
    	val key = indexMethod(e)
	    val di = map(key)  - (date, et)
	    val newMap = map + (key -> di)
	    new Index(newMap, indexMethod)
      }
    }
  }

  def === (key: IndexType, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
    map.getOrElse(key, DateIndex()).get(date, metaData)
  }
  
  def >(from: IndexType, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
    >=(from, date, metaData) -- ===(from, date, metaData)
  }
  
  def >= (from: IndexType, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
    map.from(from).values.foldLeft(Set[Entity]()) {(set, di) => set ++ di.get(date, metaData)} 
  }
  
  def <(until: IndexType, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
    map.until(until).values.foldLeft(Set[Entity]()) {(set, di) => set ++ di.get(date, metaData)}
  }
  
  def <= (value: IndexType, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
	  ===(value, date, metaData) ++ <(value, date, metaData)
  }
  
  def range(from: IndexType, until: IndexType, date: Date, metaData: CompactEntityMetaData) : Set[Entity] = {
    val dateIndexes = map.range(from, until).values
    dateIndexes.foldLeft(Set[Entity]()){(set, di) =>
      set ++ di.get(date, metaData)
    }
  }
  
}