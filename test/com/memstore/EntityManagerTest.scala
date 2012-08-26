package com.memstore

import org.junit.Test
import org.junit.Before
import org.junit.Assert._
import java.util.Date
import com.memstore.Types.Entity

class EntityManagerTest {
  
  @Test
  def testEntityManager() {
    val em = EntityManager()
    
    val t1 = new Date(10000)
    val e1 = Map[String, Any](("id" -> 1), ("a" -> 2), ("b" -> "hello"))

    val em1 = em.add("test", t1,e1)
    assertFetchById(filter(e1), em1, 1, t1)
    
    val t2 = new Date(20000)
    val e1Mark = Map[String, Any](("id" -> 1), ("a" -> 2), ("b" -> "world"))
    val em1Mark = em1.add("test", t2,e1Mark)
    assertFetchById(filter(e1Mark), em1Mark, 1, t2)
    
    val t3 = new Date(30000)
    val e2 = Map[String, Any](("id" -> 2), ("a" -> 2), ("b" -> "hello"))
    val em2 = em1Mark.add("test", t3,e2)
    assertFetchById(filter(e2), em2, 2, t3)
    
    val t4 = new Date(40000)
    val em3 = em2.remove("test", t4, 1)
    assertEquals(None, em3.get("test", 1, t4))
  }
  
  @Test
  def testNulls() {
    val em = EntityManager()
    
    val t1 = new Date(10000)
    val e1 = Map[String, Any](("id" -> 1), ("a" -> null), ("b" -> "hello"))
    val em1 = em.add("test", t1,e1)
    assertFetchById(filter(e1), em1, 1, t1)
  }
  
  private def filter(map: Map[String, Any]) : Map[String, Any] = map.filter(t => !(t._1.startsWith("_") || t._2 == null))
  
  private def assertFetchById(expectedEntity: Entity, em: EntityManager, value: Any, time: Date) {
    val foundEntity = em.get("test", value, time).get
    assertEquals(expectedEntity, foundEntity)
  } 
}