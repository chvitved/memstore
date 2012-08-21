package com.memstore.entity
import org.junit.Test
import java.util.Date
import org.junit.Assert._
import org.junit.Before

class EntityTest {

  val name = "test"
  
  @Test
  def testTemporalData() {
    val id = 1
    val entity = new EntityTimeline(name, id)
    val map1 = Map[String, Any](("id" -> id), ("a" -> "hello"), ("b" -> "world"), ("c" -> 5L))
    val map2 = Map[String, Any](("id" -> id), ("a" -> "hello1"), ("b" -> "world"))
    val t1 = new Date(1000L) 
    val t2 = new Date(2000L)
    val now = new Date()
    
    assertEquals("no value at this point in time", None, entity.get(now))
    val e2 = entity + (t1, map1, name)
    assertEquals("no value at this point in time", None, e2.get(new Date(500L)))
    assertEquals(Some(map1), e2.get(now))
    
    val e3 = e2 + (t2, map2, name)
    
    assertEquals(Some(map1), e3.get(new Date(1500L)))
    assertEquals(Some(map2), e3.get(new Date(2000L)))
    assertEquals(Some(map2), e3.get(new Date(3000L)))
  }
  
  @Test
  def testDelete() {
    val id = 1
    val entity = new EntityTimeline(name, id)
    val map = Map[String, Any](("id" -> id), ("a" -> "hello"))
    val t1 = new Date(1000L)
    
    //add value
    val e2 = entity + (t1, map, "")
    assertEquals(map, e2.get(t1).get)
    
    //delete value
    val t2 = new Date(2000L)
    val e3 = e2 - t2
    assertEquals("no value.. it has been deleted", None, e3.get(new Date()))
    
    //add value
    val map3 = Map[String, Any](("id" -> id), ("a" -> "hello world"))
    val t3 = new Date(3000L)
    val e4 = e3 + (t3, map3, name)
    assertEquals(Some(map3), e4.get(t3))
    
    //get value for different times
    assertEquals(Some(map), e4.get(t1))
    assertEquals("no value.. it has been deleted", None, e4.get(t2))
    
  }
  
  @Test(expected=classOf[Exception])
  def testNewEntitiesMustBeNewest() {
    val id = "a"
    val entity = new EntityTimeline(name, id)
    val map1 = Map[String, Any](("id" -> id))
    val map2 = Map[String, Any](("id" -> id))
    
    val t1 = new Date(2000L)
    val t2 = new Date(1000L)
    val e2 = entity + (t1, map1, name)
    e2 + (t2, map2, name)
  }
  
  @Test
  def testFilterUndercoreValues() {
    val m = Map[String, Any](("a" -> 2), ("b" -> "hello"), ("_id" -> "hello"))
    val ce = CompactEntity(name, m)
    assertFalse(ce.get.contains("_id"))
  }

}