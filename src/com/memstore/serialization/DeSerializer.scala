package com.memstore.serialization
import java.util.Date
import scala.collection.JavaConversions.asScalaBuffer
import com.memstore.Types.SecondaryIndex
import com.memstore.Types.PrimaryIndex
import com.memstore.entity.impl.cepb.CEPB
import com.memstore.entity.impl.cepb.CEPBDataPool
import com.memstore.entity.impl.cepb.CEPBMetaData
import com.memstore.entity.impl.cepb.CEPBPoolForType
import com.memstore.entity.CompactEntity
import com.memstore.entity.CompactEntityDataPool
import com.memstore.entity.CompactEntityMetaData
import com.memstore.entity.ET
import com.memstore.entity.EntityData
import com.memstore.entity.EntityManager
import com.memstore.entity.EntityTimeline
import com.memstore.entity.EntityTimelineWithNoHistory
import com.memstore.index.clustered.ClusteredMap
import com.memstore.index.factory.OrderingFactory
import com.memstore.index.factory.PrimaryIndexFactory
import com.memstore.index.factory.SecondaryIndexFactory
import com.memstore.index.DateIndex
import com.memstore.index.Mark
import com.memstore.index.TombstoneMark
import com.memstore.index.ValueMark
import com.memstore.serialization.Serialization.PBDateIndex
import com.memstore.serialization.Serialization.PBEntityTimeline
import com.memstore.serialization.Serialization.PBEntityTimelineValue
import com.memstore.serialization.Serialization.PBValue
import java.io.InputStream
import com.memstore.serialization.Serialization.Header
import com.google.protobuf.CodedInputStream
import scala.collection.convert.Wrappers.JDictionaryWrapper


object DeSerializer {

  def read(is: InputStream): EntityManager = {
	val dataPool = readDataPool(is)
	val eds = readMap(is, readString, readEntityData)
	EntityManager(eds, dataPool)
  }
  
  private def readEntityData(is: InputStream): EntityData = {
      val metaData = readCEPBMetaData(is)
      val key = readString(is)
	  EntityData(metaData, key, readPrimaryIndex(is), readSecondaryIndexes(is))
  }
  
  private def readPrimaryIndex(is: InputStream): PrimaryIndex = {
    readClusteredMap(is) match {
      case Some(map) => PrimaryIndexFactory.createFromDeserialization(map)
      case None => PrimaryIndexFactory()
    }
    
  }
  
  private def readSecondaryIndexes(is: InputStream): Map[String,SecondaryIndex] = {
    val readSecondaryIndex = (is: InputStream) => {
      readClusteredMap(is) match {
        case Some(map) => {
          val column = readString(is)
    	  SecondaryIndexFactory.createFromDeserialization(map, map.ordering, column)
        }
        case None => {
          val column = readString(is)
          com.memstore.index.SecondaryIndex(column)
        }
      }
    }
    readMap(is, readString, readSecondaryIndex)
  }
  
  private def readClusteredMap(is: InputStream): Option[ClusteredMap[Any, Array[Byte]]] = {
    val bool = readBoolean(is)
    if (bool) {
    	val ordering = getOrdering(readString(is))
    	val addMethod = (m: ClusteredMap[Any, Array[Byte]], tuple: (Any, Array[Byte])) => m + tuple
    	Some(readMap(is, readAny, readBytes, ClusteredMap[Any, Array[Byte]]()(ordering), addMethod))
    } else {
      None
    }
  }
  
  private def getOrdering(orderingClassName: String) : Ordering[Any] = {
    val ordering = orderingClassName match {
      case "String" => Ordering.String
      case "Long" => Ordering.Long
      case "Integer" => Ordering.Int
    }
    ordering.asInstanceOf[Ordering[Any]]
  }
  
  private def readCEPBMetaData(is: InputStream): CEPBMetaData = {
    val name = readString(is)
    val columnToIndexMap = readMap(is, readString, readInt)
    val reverseMap = readMap(is, readInt, readString)
    val typeMap = readMap(is, readInt, readClass)
    
    val notPooledColumnsSize = readInt(is)
    val notPooledColumns = (0 until notPooledColumnsSize).foldLeft(Set[String]()) {(set, index) =>
      set + readString(is)
    }
    CEPBMetaData(name, columnToIndexMap, reverseMap, typeMap, notPooledColumns)
  }
  
  
  
  private def readDataPool(is: InputStream): CEPBDataPool = {
    val readValue = (is: InputStream) => {
      val addMethod = (m: gnu.trove.map.hash.TObjectIntHashMap[Any], tuple: (Any, Int)) => {m.put(tuple._1, tuple._2); m}
      val map = readMap(is, readAny, readInt, new gnu.trove.map.hash.TObjectIntHashMap[Any](), addMethod)
      
      val size = readHeaderSize(is)
      val list = new java.util.ArrayList[Any]()
      for(i <- 0 until size) list.add(readAny(is))

      CEPBPoolForType(map, list)
    }
    val map = readMap(is, readClass, readValue)
    CEPBDataPool(map)
  }
  
  private def readBytes(is: InputStream): Array[Byte] = {
    readPBValue(is).getBytes().toByteArray()
  }
  
  private def readAny(is: InputStream): Any = {
    toValue(readPBValue(is))
  }
  
  private def readHeaderSize(is: InputStream): Int = {
    Header.parseDelimitedFrom(is).getSize()
  }
  
  private def readInt(is: InputStream): Int = {
    readPBValue(is).getInt()
  }
  
  private def readBoolean(is: InputStream): Boolean = {
    readPBValue(is).getBoolean()
  }
  
  private def readClass(is: InputStream): Class[_] = {
    Class.forName(readString(is))
  }
  
  private def readString(is: InputStream): String = {
    readPBValue(is).getString()
  }
  
  private def readPBValue(is: InputStream): PBValue = {
    PBValue.parseDelimitedFrom(is)
  }
  
  private def readMap[A,B, C](is: InputStream, readKey: InputStream => A, readValue: InputStream => B, startMap: C, add: (C, (A,B)) => C): C = {
    val size = readHeaderSize(is)
    val map = (0 until size).foldLeft(startMap) {(map, index) =>
     val key = readKey(is)
     val value = readValue(is)
     add(map, key -> value)
    }
    map
  }
  
  private def readMap[A,B, T](is: InputStream, readKey: InputStream => A, readValue: InputStream => B): Map[A,B] = {
    readMap(is, readKey, readValue, Map[A,B](), (m: Map[A,B], tuple: (A,B)) => m + tuple)
  }
  
  
  def toValue(pbValue: PBValue) : Any = {
    if (pbValue.hasInt) pbValue.getInt()
    else if (pbValue.hasLong) pbValue.getLong()
    else if (pbValue.hasDate) new Date(pbValue.getDate())
    else if(pbValue.hasString) pbValue.getString()
    else if(pbValue.hasTombstone) com.memstore.entity.TombStone
    else if (pbValue.hasBoolean) pbValue.getBoolean()
    else if(pbValue.hasDouble) pbValue.getDouble()
    else if(pbValue.hasFloat()) pbValue.getFloat()
    else if (pbValue.hasBytes) pbValue.getBytes().toByteArray()
    else {
      throw new Exception("got pbvalue with an unknown value..." + pbValue)
    }
  }
  
  def toDateIndex(pbdi: PBDateIndex) : DateIndex = {
    val keyList = pbdi.getKeyList().zip(pbdi.getListList())
    
    val map = keyList.foldLeft(Map[Any, List[Mark]]()) {(map, tuple) =>
      val (pbKey, pbMarkList) = tuple
      val key = toValue(pbKey);
      val tombstoneMarks = pbMarkList.getTombstoneDatesList().map(t => TombstoneMark(new Date(t)));
      val dateMarks = pbMarkList.getDatesList().map(t => ValueMark(new Date(t)));
      val sortedVector = (Vector[Mark]() ++ tombstoneMarks ++ dateMarks).sortWith{(a,b) => a.date.compareTo(b.date) < 0}
      map + (key -> sortedVector.toList)
    }
    new DateIndex(map)
  }
  
  
  def deSerializeEntityTimeline(pbet: PBEntityTimeline) : EntityTimeline = {
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
  
  private def pbEntityTimelineValueToCompactEntity(pbetV: PBEntityTimelineValue) : Option[CompactEntity] = {
    if (pbetV.hasTombstone()) None else Some(new CEPB(pbetV.getEntity().toByteArray()))
  }
  
  implicit def longToDate(v : Long): Date = new Date(v) 
}