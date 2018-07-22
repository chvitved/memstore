package com.memstore.entity

import org.junit.Test
import java.util.Date
import org.junit.Assert._
import org.junit.Before
import com.memstore.Types.Entity

class EntityWithInsertionDateTest {

  @Test
  def testWithInsertionDate() {
    val em = EntityManager()
    
    val t1 = new Date(10000)
    val e1 = Map[String, Any](("id" -> 1), ("a" -> "hello"))
    val em1 = em.add("test", t1, e1)
    
    assertEquals(t1, em1.getWithInsertionDate("test", 1, new Date()).get._2)
    
    val t2 = new Date(20000)
    val e2 = Map[String, Any](("id" -> 1), ("a" -> "hello again"))
    val em2 = em1.add("test", t2, e2)
    
    assertEquals(t1, em2.getWithInsertionDate("test", 1, new Date(15000)).get._2)
    assertEquals(t2, em2.getWithInsertionDate("test", 1, new Date()).get._2)
  }
}