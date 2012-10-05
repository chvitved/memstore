package com.memstore.serialization
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
import com.memstore.entity.EntityManager
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.CompactEntityMetaData
import com.memstore.serialization.Serialization.PBMetaData
import com.memstore.entity.impl.cepb.CEPBMetaData
import com.memstore.serialization.Serialization.PBStringIntPair
import com.memstore.serialization.Serialization.PBDataPool
import com.memstore.entity.CompactEntityDataPool
import com.memstore.entity.impl.cepb.CEPBDataPool
import com.memstore.serialization.Serialization.PBDataPoolForType
import scala.collection.JavaConversions._
import com.memstore.pbutils.PBUtils

object Serializer {
  
  def serialize(em: EntityManager): PBEntityManager = {
    val time = System.currentTimeMillis
	val pbemBuilder = PBEntityManager.newBuilder().setDatoapool(toPBDataPool(em.dataPool))
    em.map.values.foreach{ entityData =>
      val pbedBuilder = PBEntityData.newBuilder()
      pbedBuilder.setMetaData(toPBMetaData(entityData.metaData)).setKeyColumn(entityData.key)
      entityData.primaryIndex.elements.foreach{ case(key, entityTimeline) =>
        pbedBuilder.addPrimaryIndexKey(PBUtils.toPBValue(key))
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
  
  private def toPBDataPool(dataPool: CompactEntityDataPool): PBDataPool = {
    dataPool match {
      case pool: CEPBDataPool => {
        toPBDataPool(pool)
      }
      case _ => throw new Exception("could not serialize datapool " + dataPool + " of class " + dataPool.getClass)
    }
  }
  
  private def toPBDataPool(dp: CEPBDataPool): PBDataPool = {
    val builder = PBDataPool.newBuilder()
    dp.map.foreach{case (clas, poolForType) => 
      val b = PBDataPoolForType.newBuilder().setType(clas.getName())
      val orderedValues = poolForType.map.elements.toArray //todo not very efficient
      	.sortWith{(v1, v2) => v1._2 < v2._2}.map(_._1)
      orderedValues.foreach(v =>b.addValue(PBUtils.toPBValue(v)))
      builder.addPoolForType(b)
    }
    builder.build
  }
  
  private def toPBMetaData(md: CompactEntityMetaData) : PBMetaData = {
    md match {
      case m: CEPBMetaData => {
        toPBMetaData(m)
      }
      case _ => throw new Exception("could not serialize metadata " + md + " of class " + md.getClass)
    }
  }
  
  private def toPBMetaData(md: CEPBMetaData): PBMetaData = {
    val builder = PBMetaData.newBuilder()
    builder.setName(md.name)
    md.columnToIndexMap.foreach{case (columnName, index) =>
      builder.addColumnToIndex(toPBStringIntPair(columnName, index))
    }
    md.typeMap.foreach{case(index, clas) => builder.addIndexToType(toPBStringIntPair(clas.getName(), index))}
    md.notPooledColumnsName.foreach(name => builder.addNotPooledColumns(name))
    builder.build
  }
  
  private def toPBStringIntPair(string: String, int: Int) = PBStringIntPair.newBuilder().setString(string).setInt(int)
  
}