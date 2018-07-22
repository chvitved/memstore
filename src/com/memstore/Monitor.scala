package com.memstore

import com.memstore.Types.Entity

object Monitor {

  var entityMap = Map[String, List[Int]]()
  
  def addDiff(name: String, e: Entity) {
    val diffList = entityMap.getOrElse(name, List[Int]())
    entityMap += name -> (e.size :: diffList)
  }
  
  def showStats() {
    println("---- Monitor----")
    entityDiffStats()
    println()
  }
  
  private def entityDiffStats() {
    entityMap.foreach{tuple =>
      val name = tuple._1
      val size = tuple._2.size
      val avg = tuple._2.sum / size
      println(String.format("%s added %s with a diff average of %s", name, size + "", avg + ""))
    }
  } 
  
  def clear() {
    entityMap = Map[String, List[Int]]()
  }
  
  def showAndClear() {
    showStats()
    clear()
  }
}