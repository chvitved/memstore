//package com.memstore.index
//import scala.collection.immutable.SortedMap
//import scala.collection.immutable.TreeMap
//import com.memstore.entity.CompactEntity
//import java.util.Date
//
//object Index {
//
//  private def buildMap[CompactEntity,IndexType<%Ordered[IndexType]](data: Set[CompactEntity], indexMethod: (CompactEntity) => IndexType) : SortedMap[IndexType, Set[CompactEntity]] = {
//    val mutableMap = new scala.collection.mutable.HashMap[IndexType, scala.collection.mutable.ListBuffer[CompactEntity]]() {
//      override def default(key: IndexType) = {
//          val list = new scala.collection.mutable.ListBuffer[CompactEntity]()
//          put(key, list)
//          list
//      }
//    }
//    data.foreach(entity => mutableMap(indexMethod(entity)) + entity)
//    val mapWithSet = mutableMap.elements.map((tuple) => (tuple._1, Set(tuple._2.toSeq:_*)))
//    TreeMap[IndexType, Set[CompactEntity]](mapWithSet.toList.toSeq:_*)
//    //wow these lines got ugly --improve
//  }
//  
//  def create[Entity,IndexType<%Ordered[IndexType]](data: Set[CompactEntity], indexMethod: (CompactEntity) => IndexType ) = {
//    new Index(buildMap(data, indexMethod), indexMethod)
//  }
//}
//
//class Index[IndexType<%Ordered[IndexType]] (val map: SortedMap[IndexType, Set[Entity]], indexMethod: (CompactEntity) => IndexType) {
//
//  def === (key: IndexType) : Set[Entity] = {
//    map.getOrElse(key, Set[Entity]())
//  }
//  
//  def + (ce: CompactEntity, date: Date): Index[IndexType] = {
//    val key = indexMethod(ce)
//    val newMap = if (key != null) {
//    	val set = map.getOrElse(key, Set()) + ce
//    	map + (key -> set)
//    } else map //TODO we should probably not just throw away null values
//    
//    new Index(newMap, indexMethod)
//  }
//  
//  def ++ (entities: Set[CompactEntity]): Index[IndexType] = entities.foldLeft(this) {(index, entity) => index + entity}
//  
//  def - (ce: CompactEntity): Index[IndexType] = {
//    val key = indexMethod(ce)
//    val set = map(key) - ce
//    val newMap = map + (key -> set)
//    new Index(newMap, indexMethod)
//  }
//  
//  def -- (entities: Set[CompactEntity]): Index[IndexType] = entities.foldLeft(this) {(index, entity) => index - entity}
//  
//  def entities = map.foldLeft(Set[CompactEntity]()) {
//	  (set, tuple) => set ++ tuple._2
//  }
//  
//}