package com.memstore.entity
import java.util.Date
import com.memstore.Types.Entity
import com.memstore.Monitor

object EntityTimeline {
  
  def apply() : EntityTimeline = EmptyEntityTimeline()
  
  def get(date: Date, timeline: List[(Date, Option[CompactEntity])], map: Entity, metaData: CompactEntityMetaData) : Option[Entity] = {
    timeline match {
      case (eDate, ce) :: tail => {
        if (!date.before(eDate)) {
          //use this entity
          ce match {
            case Some(ce) => Some(add(map, ce.get(metaData)))
            case None => None
          }
        }else {
          //we are looking for a value older than the current one in the timeline
          val e: Entity = ce match {
            case Some(ce) => add(map, ce.get(metaData))
            case None => Map[String, Any]()
          }
          get(date, tail, e, metaData)
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
    if (newDate before currentDate) 
    	throw new Exception(String.format("date set (%s) for removal was not after the latest date (%s) in the timeline", newDate, currentDate))
  }
}

case class ET(timeline: List[(Date, Option[CompactEntity])]) extends EntityTimeline{
  
  def + (date: Date, entity: Entity, metaData: CompactEntityMetaData) : AddResult = {
    val current = get(date, metaData)
    if (!current.isEmpty && current.get == entity) AddResult(false, this, metaData)
    else {
    	val (ce, newCeMetaData) = CompactEntity(entity, metaData)
    	val (et, resultingMetaData) = timeline match {
    	  case Nil =>  (ET((date, Some(ce)) :: timeline), newCeMetaData)
    	  case head :: tail => {
    		if (date.before(head._1)) {
    			throw new Exception("data added must be the newest")
    		}
    		head._2 match {
    		  case None => (ET((date, Some(ce)) :: head :: tail), newCeMetaData)
    		  case Some(h) => {
    		    val diffMap = diff(h, entity, metaData)
				if (diffMap.isEmpty) {
					throw new Exception("we should not get here");
				} else {
					Monitor.addDiff(metaData.name, diffMap)
	    			val (ceDiff, newCeMetaData1) = CompactEntity(diffMap, newCeMetaData)
	    			(ET((date, Some(ce)) :: (head._1, Some(ceDiff)) :: tail), newCeMetaData1) 
				}
    		  } 
    		}
    	  }
    	}
    	AddResult(true, et, resultingMetaData)
    }
  }
  
  def - (date: Date) :ET = {
    if (timeline.isEmpty || timeline.head._2 == None) {
      throw new Exception(String.format("no value exists in the timeline", date, timeline.head._1))
    }
    //we can only remove with a date later than the last one
    EntityTimeline.isAfter(date, timeline.head._1)
    ET((date, None) :: timeline)
  }
  
  /**
   * first naive implementation
   * This method could probably be optimized by working directly on the compactentity itself
   */
  private def diff(old: CompactEntity, nev: Entity, metaData: CompactEntityMetaData): Entity = {
    val oldSet = old.get(metaData).elements.toSet
    val newSet = nev.elements.toSet
    val add = oldSet -- newSet
    val addKeys = add.map(_._1)
    val deleteKeys = (newSet -- oldSet).map(_._1)
    val delete = deleteKeys -- addKeys
    
    val m = add.foldLeft(Map[String, Any]()) {(map, tuple) => map + tuple}
    delete.foldLeft(m) {(map, key) =>
      map + (key -> TombStone.tombStone)
    }
  }
  
  def get(date: Date, metaData: CompactEntityMetaData) : Option[Entity] = {
      EntityTimeline.get(date, timeline, Map[String, Any](), metaData)
  }
}

abstract trait EntityTimeline {
  
  def timeline: List[(Date, Option[CompactEntity])]
  def + (date: Date, entity: Entity, ceMetaData: CompactEntityMetaData) : AddResult
  def - (date: Date) :EntityTimeline
  def get(date: Date, metaData: CompactEntityMetaData) : Option[Entity]
  def getNow(metaData: CompactEntityMetaData) : Option[Entity] = get(new Date(), metaData)
}

case class AddResult(changed: Boolean, et: EntityTimeline, ceMetaData: CompactEntityMetaData)

case class EntityTimelineWithNoHistory(date: Date, ce: CompactEntity) extends EntityTimeline {
  def timeline: List[(Date, Option[CompactEntity])] = List[(Date, Option[CompactEntity])](date -> Some(ce)) 
  def + (date: Date, entity: Entity, ceMetaData: CompactEntityMetaData): AddResult = {
    EntityTimeline.isAfter(date, this.date)
    toET() + (date, entity, ceMetaData)
  }
  def - (date: Date):EntityTimeline  = {
    EntityTimeline.isAfter(date, this.date)
	toET() - date
  }
  private def toET() = ET(List[(Date, Option[CompactEntity])]((this.date, Some(this.ce))))
  def get(date: Date, metaData: CompactEntityMetaData) : Option[Entity] = if (!(date before this.date)) Some(ce.get(metaData)) else None
}

private case class EmptyEntityTimeline extends EntityTimeline {
  def timeline: List[(Date, Option[CompactEntity])] = List[(Date, Option[CompactEntity])]()
  def + (date: Date, entity: Entity, ceMetaData: CompactEntityMetaData): AddResult = {
    val (ce, metaData) = CompactEntity(entity, ceMetaData)
    AddResult(true, EntityTimelineWithNoHistory(date, ce), metaData)
  }
  def - (date: Date):EntityTimeline = {
    throw new Exception("cannot remove from empty entitytimeline")
  }
  def get(date: Date, metaData: CompactEntityMetaData) : Option[Entity] = None
}
