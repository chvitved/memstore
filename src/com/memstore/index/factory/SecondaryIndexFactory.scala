package com.memstore.index.factory
import java.io.InputStream
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity
import com.memstore.index.clustered.ClusteredIndex
import com.memstore.index.clustered.NewClusteredIndex
import com.memstore.serialization.DeSerializer
import com.memstore.serialization.Serialization.PBDateIndex
import com.memstore.serialization.Serializer
import com.memstore.index.Index
import com.memstore.index.DateIndex
import com.memstore.index.EmptyIndex
import com.memstore.index.StandardMapIndex
import com.memstore.index.clustered.New1ClusteredIndex
import com.memstore.index.clustered.ClusteredMap
import com.memstore.index.SecondaryIndex

object SecondaryIndexFactory {
  
  def apply(): Index[Any, DateIndex] = {
    return new EmptySecondaryIndex()
  }
  
  def createFromDeserialization(clusteredMap: ClusteredMap[Any, Array[Byte]], ordering: Ordering[Any], column: String) : SecondaryIndex[Any] = {
    new SecondaryIndex[Any](new ClusteredIndex(clusteredMap, ordering, DateIndex(), valueFromBytes, valueToBytes), column)
  } 
  
  private def valueFromBytes(is: InputStream): DateIndex = {
    DeSerializer.toDateIndex(PBDateIndex.parseFrom(is))
  }
  
  private def valueToBytes(di: DateIndex): Array[Byte] = {
    Serializer.toPBDateIndex(di).toByteArray()
  }
  
  case class EmptySecondaryIndex() extends EmptyIndex[Any, DateIndex] {
    
    def create(key: Any): Index[Any, DateIndex] = {
      val ordering = OrderingFactory.create(key)
      ClusteredIndex.empty(ordering, DateIndex(), valueFromBytes, valueToBytes)
      //StandardMapIndex(DateIndex())(ordering)
      //NewClusteredIndex.empty(ordering, DateIndex(), valueFromBytes, valueToBytes)
      //New1ClusteredIndex.empty(ordering, DateIndex(), valueFromBytes, valueToBytes)
    }
    
    def empty = DateIndex()
  }

}