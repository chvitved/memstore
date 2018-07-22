package com.memstore.query
import org.junit.Test
import org.junit.Before
import org.junit.Assert._
import java.util.Date
import com.memstore.Types.Entity
import com.memstore.entity.EntityManager
import com.memstore.entity.EntityConfig

class QueryTest {
  
  val person = "person"
  val name = "name"
  val age = "age"
    
  val christian = Map[String, Any](("id" -> 1), (name -> "Christian"), (age -> 33))
  val malene = Map[String, Any](("id" -> 2), (name -> "Malene"), (age -> 32))
  val clara = Map[String, Any](("id" -> 3), (name -> "Clara"), (age -> 4))
  val anton = Map[String, Any](("id" -> 4), (name -> "Anton"), (age -> 2))
    
  var em: EntityManager = null
  var t1 = new Date(10000)
  
  @Before
  def setup() {
    val em1 = EntityManager()
    
    val em2 = em1.addEntity(EntityConfig(person,  age, name))
    
    em = List(christian, malene, clara, anton).foldLeft(em2){(em, p) => em.add(person, t1, p)}
  }

  @Test
  def testSimpleQuery() {
    val res = Query(em, "entity person where name = :1", "Christian")
	assertEquals(1, res.size)
	assertEquals(christian, res.head)
	
	val res1 = Query(em ,"entity person where age > :1", 4)
	assertEquals(2, res1.size)
	
	val res2 = Query(em, "entity person where age > :1 and name = :2", 4, "Christian")
	assertEquals(1, res2.size)
	
	val res3 = Query(em, "entity person where age > :1 or name = :2", 4, "Anton")
	assertEquals(3, res3.size)
	
	val res4 = Query(em, "entity person where age > :1 and (name = :2 or name=:3)", 2, "Anton", "Clara")
	assertEquals(1, res4.size)
	
	val res5 = Query(em, "entity person where age > :1 and (name = :2 or name=:3)", 1, "Anton", "Clara")
	assertEquals(2, res5.size)
	
	val res6 = Query(em, "entity person where age > :1 and (name = :2 or name=:3)", 5, "Anton", "Clara")
	assertEquals(0, res6.size)
	
	val res7 = Query(em, "entity person where id = :1", 1)
	assertEquals(1, res7.size)
	
	val res8 = Query(em, "entity person where age > :1", 0)
	assertEquals(4, res8.size)
  }
}