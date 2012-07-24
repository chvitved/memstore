package com.memstore
import org.junit.Test
import java.util.Date
import org.junit.Before
import org.junit.Assert._

class EntityManagerTest {
  
  @Before
  def setup() {
    EntityDescriptor.add("test", "id")
  }

  @Test
  def testEntityManager() {
    val em = EntityManager()
    
    val t1 = new Date(10000)
    val e1 = Map[String, Any](("id" -> 1), ("a" -> 2), ("b" -> "hello"))

    val em1 = em.add("test", t1,e1)
    assertFetchById(e1, em1, 1, t1)
    
    val t2 = new Date(20000)
    val e1Mark = Map[String, Any](("id" -> 1), ("a" -> 2), ("b" -> "world"))
    val em1Mark = em1.add("test", t2,e1Mark)
    assertFetchById(e1Mark, em1Mark, 1, t2)
    
    val t3 = new Date(30000)
    val e2 = Map[String, Any](("id" -> 2), ("a" -> 2), ("b" -> "hello"))
    val em2 = em1Mark.add("test", t3,e2)
    assertFetchById(e2, em2, 2, t3)
    
  }
  
  private def assertFetchById(expectedEntity: Map[String, Any], em: EntityManager, value: Any, time: Date) {
    val foundEntity = em.get("test").primaryIndex(value).get(time)
    assertEquals(expectedEntity, foundEntity)
  } 
}