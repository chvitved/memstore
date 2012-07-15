package com.memstore
import com.memstore.entity.EntityTimeline

object EntityDescriptor {
  
  var primaryKeyMap = Map[String, String]()
  
  def getKey(name: String) = primaryKeyMap(name)
  
  def contains(name: String) = primaryKeyMap.contains(name)
  
  def add(name: String, key: String) {
    primaryKeyMap += name -> key
  }
  
  

}