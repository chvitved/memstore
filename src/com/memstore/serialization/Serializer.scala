package com.memstore.serialization
import com.memstore.EntityManager
import com.memstore.serialization.Serialization.Data
import com.memstore.Types.Entity

class Serializer {
  
  def serialize(em: EntityManager) {
	val dataBuilder = Data.newBuilder()
    em.map.foreach{ case(name, entityData) =>
		val entities = Data.Entities.newBuilder().setName(name)
		entityData.primaryIndex.elements.foreach{ case(key, entityTimeline) =>
		  entityTimeline.timeline.foreach{ case(date, ce) =>
    	  	val entity = Data.Entity.newBuilder().setDate(date.getTime())
    	  	ce match {
    	  		case None => entity.setTombstone(true)
    	  		case Some(ce) => {
    	  		  serializeEntity(ce.get, entity, key)
    	  		}
    	  	}
    	  	entities.addEntity(entity)
		  }
		}
      	dataBuilder.addEntities(entities)
    }
    
    println(dataBuilder.build())
  }
  
  private def serializeEntity(e: Entity, se: Data.Entity.Builder, key: Any) {
    e.elements.foreach{case(name, value) =>
    	value match {
    	  case v: String => se.addStrings(Data.StringTuple.newBuilder().setKey(name).setValue(v))
    	  case v: Int => se.addIntegers(Data.IntTuple.newBuilder().setKey(name).setValue(v))
    	}
    }
  }

}