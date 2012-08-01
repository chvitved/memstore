package com.memstore.index

import java.util.Date
import com.memstore.entity.EntityTimeline

/**
 * This is a very naive first implementation
 */
object DateIndex {
  
  def apply(): DateIndex = new DateIndex(Map[EntityTimeline, List[Mark]]())
}

class DateIndex private (map: Map[EntityTimeline, List[Mark]]) {
  
  def +(date: Date, e: EntityTimeline) : DateIndex = {
    validate(date, e)
    val list = map.getOrElse(e, List[Mark]())
    println("updating index")
    val newList = new Mark(date) :: list 
    new DateIndex(map + (e -> newList))
  }
  
  private def validate(date: Date, e: EntityTimeline) {
    val dateList = map.getOrElse(e, List[Mark]())
    if (!dateList.isEmpty) { 
    	val first = dateList.head
    	if (first != Nil && date.before(first.date)) throw new Exception(String.format("date %s is before the last date %s", date, first.date))
    }  
    
  }
  
  def -(date: Date, e: EntityTimeline) : DateIndex = {
    validate(date, e)
    map.get(e) match {
      case Some(list) => {
        val newList = new TombstoneMark(date) :: list
        new DateIndex(map + (e -> newList))
      }
      case None => this 
    }
  }
}

class Mark(val date: Date)
class TombstoneMark(date: Date) extends Mark(date)