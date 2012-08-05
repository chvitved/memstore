package com.memstore.index

import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity
import com.memstore.index.DateIndex._

/**
 * This is a very naive first implementation
 */
object DateIndex {
  
  def apply(): DateIndex = new DateIndex(Map[EntityNameAndId, EntityTimelineWithDateList]())
  
  type EntityNameAndId = (String, Any)
  type EntityTimelineWithDateList = (EntityTimeline, List[Mark])
}

class DateIndex private (map: Map[EntityNameAndId, EntityTimelineWithDateList]) {

  def +(date: Date, e: EntityTimeline) : DateIndex = {
    validate(date, e)
    val key = etKey(e)
    val etWithList = get(e)
    val newEtWL = (e, new Mark(date) :: etWithList._2)
    new DateIndex(map + (key -> newEtWL))
  }
  
  private def etKey(e: EntityTimeline) = (e.entityName,e.id) 
  private def emptyMapValue(e: EntityTimeline) = (e, List[Mark]())
  private def get(e: EntityTimeline) = map.getOrElse(etKey(e), emptyMapValue(e))
  
  private def validate(date: Date, e: EntityTimeline) {
    val dateList = get(e)._2
    if (!dateList.isEmpty) { 
    	val first = dateList.head
    	if (first != Nil && date.before(first.date)) throw new Exception(String.format("date %s is before the last date %s", date, first.date))
    }  
    
  }
  
  def -(date: Date, e: EntityTimeline) : DateIndex = {
    validate(date, e)
    val key = etKey(e)
    map.get(key) match {
      case Some(entityTimelineWithDateList) => {
        val dateList = entityTimelineWithDateList._2
        if (dateList.head.isInstanceOf[TombstoneMark]) this
        val newList = new TombstoneMark(date) :: dateList
        new DateIndex(map + (key -> (e, newList)))
      }
      case None => this 
    }
  }
  
  def get(date: Date) : Set[Entity] = {
    map.values.foldLeft(Set[Entity]()) {(set, entityTimelineWithDateList) =>
      val et = entityTimelineWithDateList._1
      val dateList = entityTimelineWithDateList._2
      if(existsInDateList(date, dateList)) set + et.get(date)
      else set
    }
  }
  
  private def existsInDateList(date: Date, list: List[Mark]) : Boolean = {
    def existsInDateList(date: Date, list: List[Mark], lastValue: Mark) : Boolean = {
      if (list == Nil || !date.after(list.head.date)) !lastValue.isInstanceOf[TombstoneMark]
      else existsInDateList(date, list.tail, list.head)
	}
    existsInDateList(date, list, new TombstoneMark(new Date(0)))
  }
  
}

class Mark(val date: Date)
class TombstoneMark(date: Date) extends Mark(date)