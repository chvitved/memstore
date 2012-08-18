package com.memstore
import java.util.Calendar
import java.util.Date
import com.memstore.entity.TombStone
import dk.trifork.sdm.importer.takst.model.DivEnheder
import com.memstore.temp.ZipCode

object ValuePool {
  
  var allValues = Map[Any, Any]()
  
  //need special maps for numbers
  var numberMaps = Map[Class[_], Map[Any, Any]]()
  
  val types = Set[Class[_]](
	    classOf[Int], classOf[java.lang.Integer], classOf[Boolean], classOf[java.lang.Boolean], classOf[Date],
	    classOf[Calendar], classOf[DivEnheder], classOf[TombStone], classOf[java.lang.Long], classOf[Long],
	    classOf[java.lang.Double], classOf[Double], classOf[ZipCode]
  )
  
  def size = allValues.size + numberMaps.foldLeft(0) {(acc, t) => acc + t._2.size}
  
  def intern[T](v: T): T = {
    if (v.isInstanceOf[String]) {
      v.asInstanceOf[String].intern().asInstanceOf[T]
    } else if (shouldPoolValue(v)) {
    	//interesting what makes sence o pool
    	//measure this on existing pricelists
      if (v.isInstanceOf[Number]) {
        val clas = v.getClass
        val numberMap = numberMaps.getOrElse(clas, Map[Any, Any]())
        add(v, numberMap) match {
          case (v, map) => {
            numberMaps += (clas -> map)
            v
          }
        }
        
      } else {
    	  add(v, allValues) match {
    	    case (v, map) => {
    	    	allValues = map
    	    	v
    	    }
    	  }
      }
    } else {
      println("not pooled: " + v)
      v 
    }
    	
  }
  
  private def shouldPoolValue(value: Any) = {
	  types.exists(_ == value.getClass)
  }
  
  private def add[T](v: T, map: Map[Any, Any]): (T, Map[Any, Any]) = {
    map.get(v) match {
      case Some(value: T) => (value, map) 
      //case Some(value) => value.asInstanceOf[T]
      case None => {
        val newMap = map + (v -> v)
        (v, newMap)
      }
    }
  }
}