package com.memstore.serialization
import com.memstore.EntityManager
import com.memstore.Types.Entity
import com.memstore.serialization.Serialization.PBEntityManager
import com.memstore.serialization.Serialization.PBEntityData
import com.memstore.serialization.Serialization.PBValue
import com.memstore.serialization.Serialization.PBEntityTimeline
import com.memstore.serialization.Serialization.PBEntity
import com.memstore.entity.impl.cepb.CEPB
import com.memstore.serialization.Serialization.PBEntityTimelineValue
import com.memstore.serialization.Serialization.Tombstone
import com.memstore.util.Zip

object Serializer {
  
  def serialize(em: EntityManager): PBEntityManager = {
    val time = System.currentTimeMillis
	val pbemBuilder = PBEntityManager.newBuilder()
    em.map.foreach{ case(name, entityData) =>
      val pbedBuilder = PBEntityData.newBuilder()
      pbedBuilder.setName(name).setKeyColumn(entityData.key)
      entityData.primaryIndex.elements.foreach{ case(key, entityTimeline) =>
        pbedBuilder.addPrimaryIndexKey(toPBValue(key))
        val pbetBuilder = PBEntityTimeline.newBuilder()
		entityTimeline.timeline.foreach{ case(date, ceOption) =>
		  pbetBuilder.addDate(date.getTime)
		  ceOption match {
		    case None => pbetBuilder.addValue(PBEntityTimelineValue.newBuilder().setTombstone(Tombstone.newBuilder()))
		    case Some(ce) => ce match {
		      case cepb: CEPB => pbetBuilder.addValue(PBEntityTimelineValue.newBuilder().setEntity(PBEntity.parseFrom(cepb.pbeBytes)))
		      case _ => throw new Exception("Could not serialize compact entity. " + ce + " It was of class " + ce.getClass)
		    } 
		  }
        }
        pbedBuilder.addEntityTimeline(pbetBuilder)
      }
      pbemBuilder.addEntityData(pbedBuilder)
    }
	pbemBuilder.build()
  }
  
  private def toPBValue(value: Any): PBValue = {
    val vb = PBValue.newBuilder()
    value match {
      case s: String => vb.setString(s)
      case i: Int => vb.setInt(i)
      case l: Long => vb.setLong(l)
    }
    vb.build()
  }

}