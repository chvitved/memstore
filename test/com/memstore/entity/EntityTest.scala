package com.memstore.entity

import org.junit.Test
import java.util.Date
import org.junit.Assert._
import org.junit.Before

import com.memstore.Types.Entity

class EntityTest {

  @Test
  def simple() {
    val pool = CompactEntity.emptyDataPool
    val ed = EntityData(new EntityConfig("test"))
    val t1 = new Date(1000L)
    val m = Map[String, Any]("id" -> 1, "a" -> "hello")
    val (ed2, p2) = ed + (t1, m, pool)
    assertEquals(Some(m), ed2(1, p2))
  }
  
  @Test
  def testTemporalData() {
    val p1 = CompactEntity.emptyDataPool
    val id: Int = 1
    val ed = EntityData(new EntityConfig("test"))
    val map1 = Map[String, Any](("id" -> id), ("a" -> "hello"), ("b" -> "world"), ("c" -> new java.lang.Long(5)))
    val map2 = Map[String, Any](("id" -> id), ("a" -> "hello1"), ("b" -> "world"))
    val t1 = new Date(1000L) 
    val t2 = new Date(2000L)
    val now = new Date()
    
    val (ed2, p2) = ed + (t1, map1, p1)
    assertEquals("no value at this point in time", None, ed2(id, new Date(500L), p2))
    assertEquals(Some(map1), ed2(id, now, p2))
    
    val (ed3, p3) = ed2 + (t2, map2, p2)
    
    assertEquals(Some(map1), ed3(id, new Date(1500L), p3))
    assertEquals(Some(map2), ed3(id, new Date(2000L), p3))
    assertEquals(Some(map2), ed3(id, new Date(3000L), p3))
  }
  
  @Test
  def testDelete() {
    val p = CompactEntity.emptyDataPool
    val id: Int = 1
    val ed = EntityData(new EntityConfig("test"))
    val map = Map[String, Any](("id" -> id), ("a" -> "hello"))
    val t1 = new Date(1000L)
    
    //add value
    val (ed2, p2) = ed + (t1, map, p)
    assertEquals(Some(map), ed2(id, t1, p2))
    
    //delete value
    val t2 = new Date(2000L)
    val ed3 = ed2 - (t2, id, p2)
    assertEquals("no value.. it has been deleted", None, ed3(id, new Date(), p2))
    
    //add value
    val map3 = Map[String, Any](("id" -> id), ("a" -> "hello world"))
    val t3 = new Date(3000L)
    val (ed4, p4) = ed3 + (t3, map3, p2)
    assertEquals(Some(map3), ed4(id, t3, p4))
    
    //get value for different times
    assertEquals(Some(map), ed4(id, t1, p4))
    assertEquals("no value.. it has been deleted", None, ed4(id, t2, p4))
    
  }
  
  @Test(expected=classOf[Exception])
  def testNewEntitiesMustBeNewest() {
    val md = CompactEntity.emptyDataPool
    val id = "a"
    val ed = EntityData(new EntityConfig("test"))
    val map1 = Map[String, Any](("id" -> id))
    val map2 = Map[String, Any](("id" -> id))
    
    val t1 = new Date(2000L)
    val t2 = new Date(1000L)
    val (ed2, md2) = ed + (t1, map1, md)
    ed2 + (t2, map2, md2)
  }
  
}