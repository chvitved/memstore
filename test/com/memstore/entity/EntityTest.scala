package com.memstore.entity
import org.junit.Test
import java.util.Date
import org.junit.Assert._
import org.junit.Before

class EntityTest {
  
  @Test
  def testTemporalData() {
    val entity = new EntityTimeline("test")
    val map1 = Map[String, Any](("id" -> 2), ("a" -> "hello"), ("b" -> "world"), ("c" -> 5L))
    val map2 = Map[String, Any](("id" -> 2), ("a" -> "hello1"), ("b" -> "world"))
    val t1 = new Date(1000L) 
    val t2 = new Date(2000L)
    val now = new Date()
    
    assertEquals("no value at this point in time", null, entity.get(now))
    val e2 = entity + (t1, map1)
    assertEquals("no value at this point in time", null, e2.get(new Date(500L)))
    assertEquals(map1, e2.get(now))
    
    val e3 = e2 + (t2, map2)
    
    assertEquals(map1, e3.get(new Date(1500L)))
    assertEquals(map2, e3.get(new Date(2000L)))
    assertEquals(map2, e3.get(new Date(3000L)))
  }
  
  @Test(expected=classOf[Exception])
  def testNewEntitiesMustBeNewest() {
    val entity = new EntityTimeline("test")
    val map1 = Map[String, Any](("id" -> 2))
    val map2 = Map[String, Any](("id" -> 2))
    
    val t1 = new Date(2000L)
    val t2 = new Date(1000L)
    val e2 = entity + (t1, map1)
    e2 + (t2, map2)
  }
    
  

}