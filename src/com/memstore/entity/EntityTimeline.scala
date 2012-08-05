package com.memstore.entity
import java.util.Date
import com.memstore.Types.Entity
import com.memstore.Monitor

object EntityTimeline {
  private def get(date: Date, entityName: String, timeline: List[(Date, CompactEntity)], map: Entity) : Entity = {
    timeline match {
      case (eDate, ce) :: tail => {
        if (!date.before(eDate)) {
          //use this entity
          if (ce == null) null else add(map, ce.get(entityName))
        }else {
          //we are looking for a value older than the current one in the timeline
          val e = if (ce == null) Map[String, Any]() else add(map, ce.get(entityName))  
          get(date, entityName, tail, e)
        }
      }
      case Nil => null
    }
  }
  
  private def add(accumulator: Entity, delta: Entity) : Entity = {
    delta.foldLeft(accumulator){(map, tuple) =>
      val key = tuple._1
      val value = tuple._2
      value match {
        case TombStone => map - key
        case v => map + (key -> value)
      }
    }
  }
}

class EntityTimeline private(val entityName: String, val id: Any, val timeline: List[(Date, CompactEntity)]) {
  
  def this(entityName: String, id: Any) = this(entityName, id, List[(Date, CompactEntity)]())
  
  def + (date: Date, entity: Entity) : EntityTimeline = {
    val current = get(date)
    if (current == entity) this
    else {
    	val ce = CompactEntity(entityName, entity)
    	timeline match {
    	  case Nil =>  new EntityTimeline(entityName, id, (date, ce) :: timeline)
    	  case head :: tail => {
    		if (date.before(head._1)) {
    			throw new Exception("data added must be the newest");
    		}
    		if (head._2 == null) { //last value is a delete
    		  new EntityTimeline(entityName, id, (date, ce) :: head :: tail)
    		} else {
    			val diffMap = diff(head._2, ce)
				if (diffMap.isEmpty) {
					this
				} else {
					Monitor.addDiff(entityName, diffMap)
	    			val ceDiff = CompactEntity(entityName, diffMap)
	    			new EntityTimeline(entityName, id, (date, ce) :: (head._1, ceDiff) :: tail)
				}
    		}
    	  }
    	}
    }
  }
  
  def - (date: Date) :EntityTimeline = {
    if (timeline.isEmpty || timeline.head._2 == null) {
      throw new Exception(String.format("no value exists in the timeline", date, timeline.head._1))
    }
    //we can only remove with a date later than the last one
    if (timeline.head._1.after(date)) 
    	throw new Exception(String.format("date set (%s) for removal was not after the latest date (%s) in the timeline", date, timeline.head._1))
    new EntityTimeline(entityName, id, (date, null) :: timeline) //should we use a tombstone instead of null
  }
  
  private def diff(old: CompactEntity, nev: CompactEntity): Entity = {
    val oldSet = old.get(entityName).elements.toSet
    val newSet = nev.get(entityName).elements.toSet
    val add = oldSet -- newSet
    val addKeys = add.map(_._1)
    val deleteKeys = (newSet -- oldSet).map(_._1)
    val delete = deleteKeys -- addKeys
    
    val m = add.foldLeft(Map[String, Any]()) {(map, tuple) => 
    	map + tuple
    }
    delete.foldLeft(m) {(map, key) =>
      map + (key -> TombStone.tombStone)
    }
  }
  
  def get(date: Date) : Entity = {
      EntityTimeline.get(date, entityName, timeline, Map[String, Any]())
  }
  
  def getNow() : Entity = get(new Date())
}