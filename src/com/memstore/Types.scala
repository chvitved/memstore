package com.memstore
import com.memstore.entity.EntityTimeline
import com.memstore.index.clustered.ClusteredIndex
import com.memstore.index.Index
import com.memstore.index.DateIndex

object Types {

  type Entity = Map[String, Any]
  type PrimaryIndex = Index[Any, EntityTimeline]
  type SecondaryIndex = com.memstore.index.SecondaryIndex[Any]
  case class EntityTimelineWithId(et: EntityTimeline, id: Any)


}