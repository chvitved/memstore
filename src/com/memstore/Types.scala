package com.memstore
import com.memstore.index.IndexImpl
import com.memstore.entity.EntityTimeline

object Types {

  type Entity = Map[String, Any]
  type Index = IndexImpl[AnyRef]
  case class EntityTimelineWithId(et: EntityTimeline, id: Any)
}