package com.memstore.entity

import org.junit.Test
import java.util.Date
import org.junit.Assert._
import org.junit.Before

import com.memstore.Types.Entity

class EntityTest {

  @Test
  def simple() {
    val ed = EntityData(new EntityConfig("test"))
    val t1 = new Date(1000L)
    val m = Map[String, Any]("id" -> 1, "a" -> "hello")
    val ed2 = ed + (t1, m)
    assertEquals(Some(m), ed2(1))
  }
  
  @Test
  def testTemporalData() {
    val id: Int = 1
    val ed = EntityData(new EntityConfig("test"))
    val map1 = Map[String, Any](("id" -> id), ("a" -> "hello"), ("b" -> "world"), ("c" -> new java.lang.Long(5)))
    val map2 = Map[String, Any](("id" -> id), ("a" -> "hello1"), ("b" -> "world"))
    val t1 = new Date(1000L) 
    val t2 = new Date(2000L)
    val now = new Date()
    
    val ed2 = ed + (t1, map1)
    assertEquals("no value at this point in time", None, ed2(id, new Date(500L)))
    assertEquals(Some(map1), ed2(id, now))
    
    val ed3 = ed2 + (t2, map2)
    
    assertEquals(Some(map1), ed3(id, new Date(1500L)))
    assertEquals(Some(map2), ed3(id, new Date(2000L)))
    assertEquals(Some(map2), ed3(id, new Date(3000L)))
  }
  
  @Test
  def testDelete() {
    val id: Int = 1
    val ed = EntityData(new EntityConfig("test"))
    val map = Map[String, Any](("id" -> id), ("a" -> "hello"))
    val t1 = new Date(1000L)
    
    //add value
    val ed2 = ed + (t1, map)
    assertEquals(Some(map), ed2(id, t1))
    
    //delete value
    val t2 = new Date(2000L)
    val ed3 = ed2 - (t2, id)
    assertEquals("no value.. it has been deleted", None, ed3(id, new Date()))
    
    //add value
    val map3 = Map[String, Any](("id" -> id), ("a" -> "hello world"))
    val t3 = new Date(3000L)
    val ed4 = ed3 + (t3, map3)
    assertEquals(Some(map3), ed4(id, t3))
    
    //get value for different times
    assertEquals(Some(map), ed4(id, t1))
    assertEquals("no value.. it has been deleted", None, ed4(id, t2))
    
  }
  
  @Test(expected=classOf[Exception])
  def testNewEntitiesMustBeNewest() {
    val id = "a"
    val ed = EntityData(new EntityConfig("test"))
    val map1 = Map[String, Any](("id" -> id))
    val map2 = Map[String, Any](("id" -> id))
    
    val t1 = new Date(2000L)
    val t2 = new Date(1000L)
    val ed2 = ed + (t1, map1)
    ed2 + (t2, map2)
  }
  
}