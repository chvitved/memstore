package com.memstore.index
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity
import com.memstore.Types.EntityTimelineWithId
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.CompactEntityDataPool
import com.memstore.index.clustered.ClusteredIndex
import com.memstore.util.TimeUtil
import com.memstore.index.factory.SecondaryIndexFactory
import com.memstore.index.clustered.ClusteredMap

object SecondaryIndex {
  
  def apply(column: String) : SecondaryIndex[Any] = {
    new SecondaryIndex[Any](SecondaryIndexFactory(), column)
  } 
  
  def add[IndexType](date: Date, id: Any, key: Option[IndexType], index: Index[IndexType, DateIndex]): Index[IndexType, DateIndex] = {
    key match {
      case Some(k) => {
        val di = index(k) + (date, id)
		index + (k -> di)
      }
      case None => index
    } 
  }
  
  def remove[IndexType](date: Date, id: Any, key: Option[IndexType], index: Index[IndexType, DateIndex]): Index[IndexType, DateIndex] = {
    key match {
      case Some(key) if (key != null) => {
        val di = index(key) - (date, id)
        index + (key -> di)
      }
      case _ => index
    }
  }
}

case class SecondaryIndex[IndexType](val index: Index[IndexType, DateIndex], val column: String) {
  
  def + (date: Date, id: Any, e: Entity, prevEntity: Option[Entity]): SecondaryIndex[IndexType] = {
	val key = indexMethod(e)
	val prevKey = prevEntity.flatMap(indexMethod(_))
	val changed =  (key, prevKey) match {
	  case (None, None) => false
	  case (Some(v), Some(v1)) => v != v1
	  case _ => true
	}
    if (changed) {
    	val i1 = SecondaryIndex.add(date, id, key, index)
    	val i2 = SecondaryIndex.remove(date, id, prevKey, i1)
    	new SecondaryIndex(i2, column)
    } else this
  }
  
  private def indexMethod(e: Entity): Option[IndexType] = e.get(column).map(_.asInstanceOf[IndexType])
  
  def - (date: Date, id: Any, e: Entity): SecondaryIndex[IndexType] = {
    val key = indexMethod(e)
    val newIndex = SecondaryIndex.remove(date, id, key, index)
    new SecondaryIndex(newIndex, column)
  }
  
  def === (key: IndexType, date: Date) : Set[Any] = {
    val t = System.nanoTime()
    val di = index(key)
    //println("index get time " + TimeUtil.printNanos(System.nanoTime() - t))
    val t2 = System.nanoTime()
    val res = di.get(date)
    //println("dateindex get time " + TimeUtil.printNanos(System.nanoTime() - t2))
    res
  }
  
  def >(from: IndexType, date: Date) : Set[Any] = {
    >=(from, date) -- ===(from, date)
  }
  
  def >= (from: IndexType, date: Date) : Set[Any] = {
    index.from(from).values.foldLeft(Set[Any]()) {(set, di) => set ++ di.get(date)} 
  }
  
  def <(until: IndexType, date: Date) : Set[Any] = {
    index.until(until).values.foldLeft(Set[Any]()) {(set, di) => set ++ di.get(date)}
  }
  
  def <= (value: IndexType, date: Date) : Set[Any] = {
	  ===(value, date) ++ <(value, date)
  }
  
  def range(from: IndexType, until: IndexType, date: Date) : Set[Any] = {
    val dateIndexes = index.range(from, until).values
    dateIndexes.foldLeft(Set[Any]()){(set, di) =>
      set ++ di.get(date)
    }
  }
}
