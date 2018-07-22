package com.memstore.serialization
import com.memstore.Types.Entity
import com.memstore.Types.SecondaryIndex
import com.memstore.Types.PrimaryIndex
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
import com.memstore.entity.impl.cepb.CEPBMetaData
import com.memstore.entity.CompactEntityDataPool
import com.memstore.entity.impl.cepb.CEPBDataPool
import scala.collection.JavaConversions._
import com.memstore.entity.EntityTimeline
import com.memstore.entity.TombStone
import com.memstore.entity.EntityData
import com.memstore.index.DateIndex
import com.memstore.serialization.Serialization.PBDateIndex
import com.memstore.serialization.Serialization.PBDateIndex.MarkList
import com.memstore.index.Mark
import com.memstore.index.TombstoneMark
import com.memstore.index.clustered.ClusteredIndex
import com.google.protobuf.ByteString
import java.io.OutputStream
import com.memstore.serialization.Serialization.Header
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.GeneratedMessage
import com.memstore.entity.impl.cepb.CEPBPoolForType
import com.memstore.index.Index
import com.memstore.index.factory.SecondaryIndexFactory.EmptySecondaryIndex
import com.memstore.index.EmptyIndex
import java.util.Date
import gnu.trove.decorator.TObjectIntMapDecorator

object Serializer {
  
  def write(em: EntityManager, os: OutputStream) {
	writePBDataPool(em.dataPool, os)
	val map = writeMap(os, em.map, writeString _, writeEntityData _)
  }
  
  private def writeEntityData(ed: EntityData, os: OutputStream) {
    writeMetaData(ed.metaData, os)
    writeString(ed.key, os)
    writeIndex(ed.primaryIndex, os)
    writeSecondaryIndexes(ed.indexes, os)
  }
  
  private def writeSecondaryIndexes(indexes: Map[String, SecondaryIndex], os: OutputStream) {
    val writeSecondaryIndex = (si: SecondaryIndex, os: OutputStream) => {
      writeIndex(si.index, os)
      writeString(si.column, os)
    }
    writeMap(os, indexes, writeString _, writeSecondaryIndex)
  }
  
  private def writeIndex(pi: Index[Any, _], os: OutputStream) {
    pi match {
      case ci: ClusteredIndex[_, _] => writeClusteredIndex(ci, os)
      case ci: EmptyIndex[_, _] => writeBoolean(false, os)
    }
  }
  
  private def writeClusteredIndex(pi: ClusteredIndex[Any, _], os: OutputStream) {
    writeBoolean(true, os)
    writeOrdering(pi.map.ordering, os)
    writeMap(os, pi.map, writeAny _, writeBytes _)
  }
  
  private def writeOrdering[T](ordering: Ordering[T], os: OutputStream) {
    val string = ordering match {
      case Ordering.String => "String" 
      case Ordering.Int => "Integer"
      case Ordering.Long => "Long"
    }
    writeString(string, os)
  }
  
  private def writeMetaData(metaData: CompactEntityMetaData, os: OutputStream) {
    metaData match {
      case m: CEPBMetaData => {
        writeMetaData(m, os)
      }
      case _ => throw new Exception("could not serialize metadata " + metaData + " of class " + metaData.getClass)
    }
  }
  
  private def writeMetaData(metaData: CEPBMetaData, os: OutputStream) {
    writeString(metaData.name, os)
    //columnToIndexMap
    writeMap(os, metaData.columnToIndexMap, toPBValue _, toPBValue _)
    //reverseMap
    writeMap(os, metaData.reverseMap, toPBValue _, toPBValue _)
    //typeMap
    val writeValue2 = (value: Class[_]) => toPBValue(value.getName())
    writeMap(os, metaData.typeMap, toPBValue _, writeValue2)
    //notPooledColumnsName
    writeInt(metaData.notPooledColumnsName.size, os)
    metaData.notPooledColumnsName.foreach(writeString(_, os))
  }
  
  private def writeBytes(bytes: Array[Byte], os: OutputStream) {
    writeMessage(PBValue.newBuilder().setBytes(ByteString.copyFrom(bytes)).build(), os)
  }
  
  private def writeString(string: String, os: OutputStream) {
    writeMessage(PBValue.newBuilder().setString(string).build(), os)
  }
  
  private def writeBoolean(bool: Boolean, os: OutputStream) {
    writeMessage(PBValue.newBuilder().setBoolean(bool).build(), os)
  }
  
  private def writeInt(integer: Int, os: OutputStream) {
    writeMessage(PBValue.newBuilder().setInt(integer).build(), os)
  }
  
  private def writeAny(o: Any, os: OutputStream) {
    writeMessage(toPBValue(o), os)
  }
  
  private def writePBDataPool(dataPool: CompactEntityDataPool, os: OutputStream){
    dataPool match {
      case pool: CEPBDataPool => {
        writePBDataPool(pool, os)
      }
      case _ => throw new Exception("could not serialize datapool " + dataPool + " of class " + dataPool.getClass)
    }
  }
  
  private def writePBDataPool(dp: CEPBDataPool, os: OutputStream) {
    val writeKey = (c: Class[_], os: OutputStream) =>   writeString(c.getName(), os)
    
    def writeValue(poolForType: CEPBPoolForType, os: OutputStream) {
      val writeKey = (key: Any) => toPBValue(key)
      val writeValue = (value: Integer) => toPBValue(value)
      val decorator = new TObjectIntMapDecorator(poolForType.map)
      writeMap(os, decorator, writeKey, writeValue)
      
      val list = poolForType.reverseMap
      writeHeaderSize(os, list.size)
      for(o <- list) toPBValue(o).writeDelimitedTo(os)
    }
    writeMap(os, dp.map, writeKey, writeValue _)

  }
  
  private def writeMessage(msgBuilder: GeneratedMessage, os: OutputStream) {
    msgBuilder.writeDelimitedTo(os)
  }
  
  
  private def writeMap[A,B](os: OutputStream, tuples: Iterable[(A,B)], writeKey: A => GeneratedMessage, writeValue: B => GeneratedMessage) {
    val wKey = (key: A, os: OutputStream)  => writeMessage(writeKey(key), os)
    val wValue = (value: B, os: OutputStream)  => writeMessage(writeValue(value), os)
    writeMap(os, tuples, wKey, wValue)
  }
  
  private def writeMap[A,B](os: OutputStream, tuples: Iterable[(A,B)], writeKey: (A, OutputStream) => Unit, writeValue: (B, OutputStream) => Unit) {
    writeHeaderSize(os, tuples.size)
    tuples.foreach{case (key, value) => 
      writeKey(key, os)
      writeValue(value, os)
    }
  }
  
  private def writeHeaderSize(os: OutputStream, size: Int) {
    writeMessage(Header.newBuilder().setSize(size).build, os)
  }
  
  def toPBValue(value: Any): PBValue = {
    val vb = PBValue.newBuilder()
    value match {
      case s: String => vb.setString(s)
      case i: Int => vb.setInt(i)
      case l: Long => vb.setLong(l)
      case d: Date => vb.setDate(d.getTime())
      case TombStone => vb.setTombstone(Tombstone.newBuilder.build)
      case b: Boolean => vb.setBoolean(b)
      case d: Double => vb.setDouble(d)
      case f: Float => vb.setFloat(f)
      case bytes: Array[Byte] => vb.setBytes(ByteString.copyFrom(bytes))
    }
    vb.build()
  }
  
  def toPBEntityTimeline(et: EntityTimeline) : PBEntityTimeline = {
    val pbetBuilder = PBEntityTimeline.newBuilder()
	et.timeline.foreach{ case(date, ceOption) =>
	  pbetBuilder.addDate(date.getTime)
	  ceOption match {
	    case None => pbetBuilder.addValue(PBEntityTimelineValue.newBuilder().setTombstone(Tombstone.newBuilder()))
	    case Some(ce) => ce match {
	      case cepb: CEPB => pbetBuilder.addValue(PBEntityTimelineValue.newBuilder().setEntity(PBEntity.parseFrom(cepb.pbeBytes)))
	      case _ => throw new Exception("Could not serialize compact entity. " + ce + " It was of class " + ce.getClass)
	    } 
	  }
    }
    pbetBuilder.build()
  }
  
  def toPBDateIndex(di: DateIndex) : PBDateIndex = {
    val dib = PBDateIndex.newBuilder()
    for((id, list) <- di.map.iterator) {
      dib.addKey(toPBValue(id))
      val lb = MarkList.newBuilder()
      for(mark <- list) {
        mark match {
          case tm: TombstoneMark => lb.addTombstoneDates(tm.date.getTime())
          case m: Mark => lb.addDates(m.date.getTime())
        }
      }
      dib.addList(lb)
    }
    dib.build
  }

}