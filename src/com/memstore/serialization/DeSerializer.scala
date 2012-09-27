package com.memstore.serialization
import com.memstore.serialization.Serialization.PBEntityManager
import com.memstore.serialization.Serialization.PBEntityTimeline
import com.memstore.entity.EntityTimeline
import java.util.Date
import com.memstore.entity.EntityTimelineWithNoHistory
import com.memstore.serialization.Serialization.PBEntityTimelineValue
import com.memstore.entity.impl.cepb.CEPB
import com.memstore.entity.CompactEntity
import com.memstore.entity.ET
import scala.collection.JavaConversions._
import com.memstore.serialization.Serialization.PBEntityData
import com.memstore.Types.{Entity, Index}
import com.memstore.serialization.Serialization.PBValue
import com.memstore.entity.EntityManager
import com.memstore.entity.EntityData
import com.memstore.serialization.Serialization.PBMetaData
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.impl.cepb.CEPBMetaData


object DeSerializer {

  def deSerialize(pbem: PBEntityManager): EntityManager = {
    val map = pbem.getEntityDataList().foldLeft(Map[String, EntityData]()) {(map, pbed) =>
      val ed = deSerializeEntityData(pbed)
      map + (ed.metaData.name -> ed)
    }
    EntityManager(map)
  }
  
  private def deSerializeEntityData(pbed: PBEntityData) : EntityData = {
    val primaryIndexMap = (0 until pbed.getEntityTimelineCount()).foldLeft(Map[Any, EntityTimeline]()) {(map, index) => 
      map + (pbValueToValue(pbed.getPrimaryIndexKey(index)) -> deSerializeEntityTimeline(pbed.getEntityTimeline(index)))
    }
    EntityData(deSerializeMetaData(pbed.getMetaData()), pbed.getKeyColumn(), primaryIndexMap,Map[String, Index]())
  }
  
  private def deSerializeMetaData(pbmd: PBMetaData): CompactEntityMetaData = {
    val start = (Map[Int, String](), Map[String, Int]())
    val (indexToColumnMap, columnToIndexMap) = pbmd.getColumnToIndexList().foldLeft(start) { (mapTuple, stringIndexPair) =>
      (
          mapTuple._1 + (stringIndexPair.getInt() -> stringIndexPair.getString()), 
          mapTuple._2 + (stringIndexPair.getString() -> stringIndexPair.getInt())
      )
    }
    val typeMap = pbmd.getIndexToTypeList().foldLeft(Map[Int, Class[_]]()) { (map, stringIndexPair) =>
      map + (stringIndexPair.getInt() -> Class.forName(stringIndexPair.getString()))
    }
    CEPBMetaData(pbmd.getName(), columnToIndexMap, indexToColumnMap, typeMap, pbmd.getNotPooledColumnsList().toSet)
  }
  
  private def deSerializeEntityTimeline(pbet: PBEntityTimeline) : EntityTimeline = {
    if (pbet.getValueCount() == 1) {
      val pbetV = pbEntityTimelineValueToCompactEntity(pbet.getValue(0))
      new EntityTimelineWithNoHistory(pbet.getDate(0), pbetV.get)
    } else if (pbet.getValueCount() > 1) {
      val timeline = ((pbet.getDateCount()-1) to 0 by -1).foldLeft(List[(Date, Option[CompactEntity])]()) {(list, index) =>
        val date: Date = pbet.getDate(index)
        val ce = pbEntityTimelineValueToCompactEntity(pbet.getValue(index))
        (date, ce) :: list
      }
      ET(timeline)
    } else throw new Exception("we should not have serialized an empty entitytimeline")
  }
  
  private def pbValueToValue(pbValue: PBValue) : Any = {
    if (pbValue.hasInt) pbValue.getInt()
    else if (pbValue.hasLong) pbValue.getLong()
    else if(pbValue.hasString) pbValue.getString()
    else {
      throw new Exception("got pbvalue with an unknown value..." + pbValue)
    }
  }
  
  private def pbEntityTimelineValueToCompactEntity(pbetV: PBEntityTimelineValue) : Option[CompactEntity] = {
    if (pbetV.hasTombstone()) None else Some(new CEPB(pbetV.getEntity().toByteArray()))
  }
  
  implicit def longToDate(v : Long): Date = new Date(v) 
}