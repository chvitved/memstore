package com.memstore.query
import org.junit.Test
import org.junit.Before
import com.memstore.EntityManager
import java.util.Date
import com.memstore.IndexConfig
import com.memstore.Types.Entity
import com.memstore.EntityConfig

class QueryTest {
  
  val person = "person"
  val name = "name"
  val age = "age"
    
  var em: EntityManager = null
  
  @Before
  def setup() {
    val em1 = EntityManager()
    
    
    val ageIndexConfig = new IndexConfig(age, (e: Entity) => e(age).asInstanceOf[Int])
    val nameIndexConfig = new IndexConfig(name, (e: Entity) => e(name).asInstanceOf[String])
    
    val em2 = em1.addEntity(new EntityConfig(person, "id", ageIndexConfig, nameIndexConfig))
    
    val t1 = new Date(10000)
    val christian = Map[String, Any](("id" -> 1), (name -> "Christian"), (age -> 33))
    val malene = Map[String, Any](("id" -> 2), (name -> "Malene"), (age -> 32))
    val clara = Map[String, Any](("id" -> 3), (name -> "Clara"), (age -> 4))
    val anton = Map[String, Any](("id" -> 4), (name -> "Anton"), (age -> 2))
    
    em = List(christian, malene, clara, anton).foldLeft(em2){(em, p) => em.add(person, t1, p)}
  }

  @Test
  def testSimpleQuery() {
//	  Query("person").where("name", eq, "Christian")
//	  Query("person").where("age", biggerthan, 4)
//	  Query("person").where("age", biggerthan, 4).and("name", eq, "Christian")
//	  Query("person").where("age", biggerthan, 4).or("name", eq ,"Clara", or("name" = "Anton"))
	  
  }
}