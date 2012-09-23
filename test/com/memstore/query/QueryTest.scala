package com.memstore.query
import org.junit.Test
import org.junit.Before
import org.junit.Assert._
import java.util.Date
import com.memstore.Types.Entity
import com.memstore.entity.EntityManager
import com.memstore.entity.IndexConfig
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
    
    
    val ageIndexConfig = new IndexConfig(age, (e: Entity) => e(age).asInstanceOf[Int])
    val nameIndexConfig = new IndexConfig(name, (e: Entity) => e(name).asInstanceOf[String])
    
    val em2 = em1.addEntity(new EntityConfig(person, "id", ageIndexConfig, nameIndexConfig))
    
    em = List(christian, malene, clara, anton).foldLeft(em2){(em, p) => em.add(person, t1, p)}
  }

  @Test
  def testSimpleQuery() {
    val res = Query("entity person where name = :1", Array[Any]("Christian"), em, new Date())
	assertEquals(1, res.size)
	assertEquals(christian, res.head)
	
	val res1 = Query("entity person where age > :1", Array[Any](4), em, new Date())
	assertEquals(2, res1.size)
	
	val res2 = Query("entity person where age > :1 and name = :2", Array[Any](4, "Christian"), em, new Date())
	assertEquals(1, res2.size)
	
	val res3 = Query("entity person where age > :1 or name = :2", Array[Any](4, "Anton"), em, new Date())
	assertEquals(3, res3.size)
	
	val res4 = Query("entity person where age > :1 and (name = :2 or name=:3)", Array[Any](2, "Anton", "Clara"), em, new Date())
	assertEquals(1, res4.size)
	
	val res5 = Query("entity person where age > :1 and (name = :2 or name=:3)", Array[Any](1, "Anton", "Clara"), em, new Date())
	assertEquals(2, res5.size)
	
	val res6 = Query("entity person where age > :1 and (name = :2 or name=:3)", Array[Any](5, "Anton", "Clara"), em, new Date())
	assertEquals(0, res6.size)
  }
}