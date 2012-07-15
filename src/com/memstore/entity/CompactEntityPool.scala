package com.memstore.entity

object CompactEntityPool {
  
  var allEntities = scala.collection.mutable.Map[CompactEntity, (Int, CompactEntity)]()
  
  def intern(e: CompactEntity): CompactEntity = {
    allEntities.get(e) match {
      case Some((counter, ce)) => {
        allEntities += ce -> (counter + 1, ce) 
        ce
      }
      case None => {
        allEntities += e -> (1, e)
        e
      }
    }
  }
  
  def remove(e: CompactEntity) {
    allEntities.get(e) match {
      case Some((counter, ce)) => {
        if (counter > 1) {
          allEntities += ce -> (counter - 1, ce)
        } else {
          allEntities = allEntities - e
        }
      }
      case None => //do nothing 
    }
  }
}