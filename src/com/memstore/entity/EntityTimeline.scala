package com.memstore.entity
import java.util.Date
import com.memstore.Types.Entity
import com.memstore.Monitor

object EntityTimeline {
  
  def apply() : EntityTimeline = new EmptyEntityTimeline()
  
  def get(date: Date, timeline: List[(Date, Option[CompactEntity])], map: Entity) : Option[Entity] = {
    timeline match {
      case (eDate, ce) :: tail => {
        if (!date.before(eDate)) {
          //use this entity
          ce match {
            case Some(ce) => Some(add(map, ce.get))
            case None => None
          }
        }else {
          //we are looking for a value older than the current one in the timeline
          val e: Entity = ce match {
            case Some(ce) => add(map, ce.get)
            case None => Map[String, Any]()
          }
          get(date, tail, e)
        }
      }
      case Nil => None
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
  
  def isAfter(newDate: Date, currentDate: Date) {
    if (!(currentDate before newDate)) 
    	throw new Exception(String.format("date set (%s) for removal was not after the latest date (%s) in the timeline", newDate, currentDate))
  }
}

private class ET(val timeline: List[(Date, Option[CompactEntity])]) extends EntityTimeline{
  
  def + (date: Date, entity: Entity, entityName: String) : ET = {
    val current = get(date)
    if (current == entity) this
    else {
    	val ce = CompactEntity(entityName, entity)
    	timeline match {
    	  case Nil =>  new ET((date, Some(ce)) :: timeline)
    	  case head :: tail => {
    		if (date.before(head._1)) {
    			throw new Exception("data added must be the newest");
    		}
    		head._2 match {
    		  case None => new ET((date, Some(ce)) :: head :: tail)
    		  case Some(h) => {
    		    val diffMap = diff(h, ce)
				if (diffMap.isEmpty) {
					this
				} else {
					Monitor.addDiff(entityName, diffMap)
	    			val ceDiff = CompactEntity(entityName, diffMap)
	    			new ET((date, Some(ce)) :: (head._1, Some(ceDiff)) :: tail)
				}
    		  } 
    		}
    	  }
    	}
    }
  }
  
  def - (date: Date) :ET = {
    if (timeline.isEmpty || timeline.head._2 == None) {
      throw new Exception(String.format("no value exists in the timeline", date, timeline.head._1))
    }
    //we can only remove with a date later than the last one
    EntityTimeline.isAfter(date, timeline.head._1)
    new ET((date, None) :: timeline)
  }
  
  private def diff(old: CompactEntity, nev: CompactEntity): Entity = {
    val oldSet = old.get.elements.toSet
    val newSet = nev.get.elements.toSet
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
  
  def get(date: Date) : Option[Entity] = {
      EntityTimeline.get(date, timeline, Map[String, Any]())
  }
}

abstract trait EntityTimeline {
  
  def timeline: List[(Date, Option[CompactEntity])]
  def + (date: Date, entity: Entity, entityName: String) : EntityTimeline
  def - (date: Date) :EntityTimeline
  def get(date: Date) : Option[Entity]
  def getNow() : Option[Entity] = get(new Date())
}

private class EntityTimelineWithNoHistory(val date: Date, ce: CompactEntity) extends EntityTimeline {
  def timeline: List[(Date, Option[CompactEntity])] = List[(Date, Option[CompactEntity])](date -> Some(ce)) 
  def + (date: Date, entity: Entity, entityName: String): EntityTimeline = {
    EntityTimeline.isAfter(date, this.date)
    toET() + (date, entity, entityName)
  }
  def - (date: Date):EntityTimeline  = {
    EntityTimeline.isAfter(date, this.date)
	toET() - date
  }
  private def toET() = new ET(List[(Date, Option[CompactEntity])]((this.date, Some(this.ce))))
  def get(date: Date) : Option[Entity] = if (!(date before this.date)) Some(ce.get) else None
}

private class EmptyEntityTimeline extends EntityTimeline {
  def timeline: List[(Date, Option[CompactEntity])] = List[(Date, Option[CompactEntity])]()
  def + (date: Date, entity: Entity, entityName: String): EntityTimeline = new EntityTimelineWithNoHistory(date, CompactEntity(entityName, entity))
  def - (date: Date):EntityTimeline = {
    throw new Exception("cannot remove from empty entitytimeline")
  }
  def get(date: Date) : Option[Entity] = None
}
