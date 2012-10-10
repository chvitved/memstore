package com.datagenerator

import java.util.Date
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity
import com.memstore.serialization.Serializer
import com.memstore.query.Query
import com.memstore.entity.impl.cepb.CEPBMetaData
import com.memstore.util.Zip
import com.memstore.serialization.DeSerializer
import com.memstore.entity.EntityManager
import com.memstore.entity.EntityConfig
import com.memstore.serialization.Serialization.PBEntityManager
import scala.util.Random

object PersonGenerator extends App{
  
  run()
  
  var highestPersonId: Long = 0
  
  def run() {
    
//	  val zipIndexConfig = new IndexConfig("zip", (e: Entity) => e("zip").asInstanceOf[Int])
//	  val nameIndexConfig = new IndexConfig("firstName", (e: Entity) => e("firstName").asInstanceOf[String])
//	  val e = EntityManager().addEntity(new EntityConfig("person", "id", zipIndexConfig, nameIndexConfig))
	  val em = EntityManager()
	  val em0 = em.addEntity(new EntityConfig("person", List("id")))
	  val em1 = em0.addEntity(new EntityConfig("children", List("id", "parent", "child")))
	  val em2 = generate(1000, em1)
	  MemUtil.printMem()

	  println("persons: " + em2.map("person").primaryIndex.size)
	  val t = System.currentTimeMillis
	  em2.fullScan("person", new Date(), e => true)
	  println("person fullscan took " + (System.currentTimeMillis - t) + " millis")
	  
	  println("children: " + em2.map("children").primaryIndex.size)
	  em2.fullScan("children", new Date(), e => true)
	  println("children fullscan took " + (System.currentTimeMillis - t) + " millis")
	  
	  
//	  val t = System.currentTimeMillis
//	  val pbemBytes = Serializer.serialize(em2).toByteArray()
//	  println("serialization took " + (System.currentTimeMillis - t))
//	  println("zipped size " + Zip.zip(pbemBytes).size)
//	  val t1 = System.currentTimeMillis
//	  DeSerializer.deSerialize(PBEntityManager.parseFrom(pbemBytes))
//	  println("de-serialization took " + (System.currentTimeMillis - t1))
	  
	  
//	  for(i <- 0 to 1000) {
//		  val res = Query("entity person where id = :1", Array[Any](scala.util.Random.nextInt(highestPersonId)), em, new Date())
//		  println("id query size " + res.size)
//		  val res1 = Query("entity person where firstName = :1", Array[Any](LoadData.firstName), em, new Date())
//		  println("firstname query size " + res1.size)
//		  val res2 = Query("entity person where zip = :1", Array[Any](LoadData.zipCode.zip), em, new Date())
//		  println("zip query size " + res2.size)
//	  }
  }
  
  object O extends Ordering[(Int, EntityTimeline)] {
	  def compare(a:(Int, EntityTimeline), b:(Int, EntityTimeline)) = a._1 compare b._1
  }
  
  def generate(number: Long, em: EntityManager) : EntityManager = {
    val d = new Date()
    Range.Long(0,number,1).foldLeft(em) {(em, id) =>
      if (id % 100000 == 0) {
    	  println("generated " + id + " persons")
    	  MemUtil.printMem()
    	  //new Serializer().serialize(em)
      }
      highestPersonId = id
      val em1 = em.add("person", d, generatePerson(id))
      generateChildrenRelation(id).foldLeft(em1) {(em, e) =>
      	em.add("children", d, e)
      }
    }

	//creating just entitytimelines => approx 20 meg per 100.000 persons
    //var m = Vector[(Int, EntityTimeline)]() //23meg
//    var m = TreeSet[(Int, EntityTimeline)]()(O) // 25meg
//    var m = Map[Int, EntityTimeline]() // 27meg
//    Range(0,number).foldLeft(m) {(m, i) =>
//      if (i % 100000 == 0) {
//    	  println("generated " + i + " persons")
//    	  MemUtil.printMem()
//      }
//      	val entity = generatePerson(i)
//		val primaryKey = "_id"
//		val value = ValuePool.intern(entity(primaryKey)).asInstanceOf[Int]
//      	val et = EntityTimeline() + (d, entity, "person")
//      	m + (value -> et)
//		//val et = m.getOrElse(value, new EntityTimeline("person", value)) + (d, entity)
//    }
//    em
    
    
//     var m = TreeSet[(Int, EntityTimeline)]()(O) // 25meg
//    var m = Map[Int, EntityTimeline]() // 27meg
//    Range(0,number).foldLeft(m) {(m, i) =>
//      if (i % 100000 == 0) {
//    	  println("generated " + i + " persons")
//    	  MemUtil.printMem()
//      }
//      	val entity = generatePerson(i)
//		val primaryKey = "_id"
//		val value = entity(primaryKey).asInstanceOf[Int]
//      	val et = EntityTimeline() + (d, entity, "person")
//      	m + (value -> et)
//		//val et = m.getOrElse(value, new EntityTimeline("person", value)) + (d, entity)
//    }
//    em
  
//creating just compactentities => approx 10 meg per 100.000 persons    
//    var s = Set[CompactEntity]()
//    Range(0,number).foldLeft(s) {(s, i) =>
//      if (i % 100000 == 0) {
//    	  println("generated " + i + " persons")
//    	  MemUtil.printMem()
//      }
//      s + CompactEntity("person", generatePerson(i))
//    }
//    em
  }
  
  def generatePerson(id: Long): Entity = {
   val zip = LoadData.zipCode
   val m = Map[String, Any]("id" -> id,
       "firstName" -> LoadData.firstName, "middleName" -> LoadData.middleName, "lastname" -> LoadData.sirName,
       "street" -> LoadData.roadName, "streetNumber" -> LoadData.roadNumber,
       "zip" -> zip.zip, "city" -> zip.city 
   )
   m
  }
  
  def generateChildrenRelation(id: Long): List[Entity] = {
    if (id > 0) {
    	val mom = Random.nextLong() % id
    	val dad = Random.nextLong() % id
    	List(Map("id" -> Random.nextLong(), "parent" -> mom, "child" -> id),Map("id" -> Random.nextLong(), "parent" -> dad, "child" -> id))
    } else List()
  }
  
}