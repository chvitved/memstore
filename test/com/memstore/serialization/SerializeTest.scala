package com.memstore.serialization

import org.junit.Test
import org.junit.Assert._
import com.memstore.entity.EntityManager
import java.util.Date

class SerializeTest {
  
  @Test
  def serialize {
    val em = EntityManager()
    val a = "a"
    val b = "b"

    val t1 = new Date(10000)
    val t2 = new Date(20000)
    val t3 = new Date(30000)
    
    val a1 = Map[String, Any]("id"->1, "a"->"hello", "b"-> 3L)
    val a1Mark = Map[String, Any]("id"->1, "a"->"hello1", "b"-> 5L)
    val a1MarkMark = Map[String, Any]("id"->1, "a"->"hello2", "b"-> 5L, "c"->4)
    
    val a2 = Map[String, Any]("id"->2, "a"->"hello")
    val a2Mark = Map[String, Any]("id"->2, "a"->"hello1", "c"-> 5L)
    
    val em1 = em.add(a, t1, a1).add(a, t2, a1Mark).add(a, t3, a1MarkMark)
    val em2 = em1.add(a, t1, a2).add(a, t2, a2Mark)
    

    val b1 = Map[String, Any]("id"->1, "b"->"hello")
    val b1Mark = Map[String, Any]("id"->1, "b"->"hellob")
    
    val b2 = Map[String, Any]("id"->2, "b"->5)
    
    val em3 = em2.add(b, t1, b1).add(b, t2, b1Mark).remove(b, t3, 1) //note also a delete
    val em4 = em3.add(b, t1, b2)
    
    val pbem = Serializer.serialize(em4)
   
    val restoredEm = DeSerializer.deSerialize(pbem)
    
    assertEquals(em4, restoredEm)
   
    
  }

}