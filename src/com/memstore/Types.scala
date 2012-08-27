package com.memstore
import com.memstore.entity.EntityTimeline

object Types {

  type Entity = Map[String, Any]
  type Index = index.Index[Any]
  case class EntityTimelineWithId(et: EntityTimeline, id: Any)


}