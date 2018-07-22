package com.memstore.index

import java.util.Date

/**
 * This is a very naive first implementation
 */
object DateIndex {
  
  def apply(): DateIndex = new DateIndex(Map[Any, List[Mark]]())
  
}

case class DateIndex(val map: Map[Any, List[Mark]]) {

  def +(date: Date, id: Any) : DateIndex = {
	val list = get(id)
    validate(date, list)
    new DateIndex(map + (id -> (ValueMark(date) :: list)))
  }
  
  private def get(id: Any) = map.getOrElse(id, List())

  
  private def validate(date: Date, dateList: List[Mark]) {
    if (!dateList.isEmpty) { 
    	val first = dateList.head
    	if (first != Nil && date.before(first.date)) throw new Exception(String.format("date %s is before the last date %s", date, first.date))
    }  
  }
  
  def -(date: Date, id: Any) : DateIndex = {
    val list = get(id)
    validate(date, list)
    list match {
      case (head :: tail) => {
        head match {
          case TombstoneMark(date) => this
          case ValueMark(date) => {
        	val newList = TombstoneMark(date) :: list
        	new DateIndex(map + (id -> newList))
          }
        }
      }
      case Nil => throw new Exception("trying to remove a value that does not exist") //should we just return this
    }
  }
  
  def get(date: Date) : Set[Any] = {
    map.iterator.foldLeft(Set[Any]()) {(set, t) =>
      val (id, dateList)  = t
      if(existsInDateList(date, dateList)) set + id
      else set
    }
  }
  
  private def existsInDateList(date: Date, list: List[Mark]) : Boolean = {
    def existsInDateList(date: Date, list: List[Mark], lastValue: Mark) : Boolean = {
      if (list == Nil || !date.after(list.head.date)) !lastValue.isInstanceOf[TombstoneMark]
      else existsInDateList(date, list.tail, list.head)
	}
    existsInDateList(date, list, TombstoneMark(new Date(0)))
  }
  
}

abstract class Mark {
  val date: Date
}

case class ValueMark(date: Date) extends Mark
case class TombstoneMark(date: Date) extends Mark