package com.memstore.entity
import org.junit.Test
import java.util.Date
import org.junit.Assert._
import org.junit.Before

class EntityTest {
  
  @Test
  def testTemporalData() {
    val id = 1
    val entity = new EntityTimeline("test", id)
    val map1 = Map[String, Any](("id" -> id), ("a" -> "hello"), ("b" -> "world"), ("c" -> 5L))
    val map2 = Map[String, Any](("id" -> id), ("a" -> "hello1"), ("b" -> "world"))
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
  
  @Test
  def testDelete() {
    val id = 1
    val entity = new EntityTimeline("test", id)
    val map = Map[String, Any](("id" -> id), ("a" -> "hello"))
    val t1 = new Date(1000L)
    
    //add value
    val e2 = entity + (t1, map)
    assertEquals(map, e2.get(t1))
    
    //delete value
    val t2 = new Date(2000L)
    val e3 = e2 - t2
    assertNull("no value.. it has been deleted", e3.get(new Date()))
    
    //add value
    val map3 = Map[String, Any](("id" -> id), ("a" -> "hello world"))
    val t3 = new Date(3000L)
    val e4 = e3 + (t3, map3)
    assertEquals(map3, e4.get(t3))
    
    //get value for different times
    assertEquals(map, e4.get(t1))
    assertNull("no value.. it has been deleted", e4.get(t2))
    
  }
  
  @Test(expected=classOf[Exception])
  def testNewEntitiesMustBeNewest() {
    val id = "a"
    val entity = new EntityTimeline("test", id)
    val map1 = Map[String, Any](("id" -> id))
    val map2 = Map[String, Any](("id" -> id))
    
    val t1 = new Date(2000L)
    val t2 = new Date(1000L)
    val e2 = entity + (t1, map1)
    e2 + (t2, map2)
  }
  
  @Test
  def testFilterUndercoreValues() {
    val m = Map[String, Any](("a" -> 2), ("b" -> "hello"), ("_id" -> "hello"))
    val ce = CompactEntity("test", m)
    assertFalse(ce.get("test").contains("_id"))
  }

}