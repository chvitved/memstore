package com.memstore.index.factory
import com.memstore.entity.EntityTimeline
import java.io.InputStream
import com.memstore.serialization.Serialization.PBEntityTimeline
import com.memstore.serialization.DeSerializer
import com.memstore.serialization.Serializer
import com.memstore.index.clustered.ClusteredIndex
import com.memstore.index.clustered.NewClusteredIndex
import com.memstore.index.Index
import com.memstore.index.EmptyIndex
import com.memstore.index.StandardMapIndex
import com.memstore.index.clustered.New1ClusteredIndex
import com.memstore.index.clustered.ClusteredMap

object PrimaryIndexFactory{

  def apply(): Index[Any, EntityTimeline] = {
    new EmptyPrimaryIndex()
  }
  
  def createFromDeserialization(clusteredMap: ClusteredMap[Any, Array[Byte]]) : ClusteredIndex[Any, EntityTimeline] = {
    new ClusteredIndex(clusteredMap, clusteredMap.ordering, EntityTimeline(), valueFromBytes, valueToBytes)
  } 
  
  private def valueFromBytes(is: InputStream): EntityTimeline = {
    DeSerializer.deSerializeEntityTimeline(PBEntityTimeline.parseFrom(is))
  }
  
  private def valueToBytes(et: EntityTimeline): Array[Byte] = {
    Serializer.toPBEntityTimeline(et).toByteArray()
  }
  
  case class EmptyPrimaryIndex() extends EmptyIndex[Any, EntityTimeline] {
    
    def create(key: Any): Index[Any, EntityTimeline] = {
      val ordering = OrderingFactory.create(key)
      ClusteredIndex.empty(ordering, EntityTimeline(), valueFromBytes, valueToBytes)
      //StandardMapIndex(EntityTimeline())(ordering)
      //NewClusteredIndex.empty(ordering, EntityTimeline(), valueFromBytes, valueToBytes)
      //New1ClusteredIndex.empty(ordering, EntityTimeline(), valueFromBytes, valueToBytes)
    }
    
    def empty = EntityTimeline()
  }
}