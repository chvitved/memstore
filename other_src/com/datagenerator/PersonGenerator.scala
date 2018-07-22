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
import scala.util.Random
import com.memstore.index.clustered.ClusteredIndex
import scala.math.Ordering.LongOrdering
import com.memstore.index.Index
import com.memstore.util.TimeUtil
import java.io.FileOutputStream
import java.io.FileInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.GZIPInputStream
import java.io.File
import java.io.InputStream
import com.google.protobuf.CodedInputStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream

object PersonGenerator extends App{
  
  run()
  
  var highestPersonId: Long = 0
  
  def run() {
    
      val entitiesToCreate = 1000000
    
//	  val zipIndexConfig = new IndexConfig("zip", (e: Entity) => e("zip").asInstanceOf[Int])
//	  val nameIndexConfig = new IndexConfig("firstName", (e: Entity) => e("firstName").asInstanceOf[String])
//	  val e = EntityManager().addEntity(new EntityConfig("person", "id", zipIndexConfig, nameIndexConfig))
	  val em = EntityManager()
	  val em0 = em.addEntity(EntityConfig("person", List("id"), List("lastName")))
	  val em1 = em0.addEntity(EntityConfig("children", List("id", "parent", "child"), List("parent")))
	  val em2 = generate(entitiesToCreate, em1)
	  MemUtil.printMem()

	  println("persons: " + em2.map("person").primaryIndex.size)
//	  val t = System.currentTimeMillis
//	  em2.fullScan("person", new Date(), e => true)
//	  println("person fullscan took " + (System.currentTimeMillis - t) + " millis")
	  
	  {
		
//		println("starting queries")
//		for(i <- 0 to 1) new Thread() {
//		  override def run() {
//		    queries()
//		  }
//		}.start
		
		//new Thread(){ override def run() {secondaryIndexQueriesPerson(em2)}}.start
		//new Thread(){ override def run() {secondaryIndexQueriesChildren(em2)}}.start
        primaryIndex(em2)
		secondaryIndexQueriesPerson(em2)
		secondaryIndexQueriesChildren(em2)
		fullscan(em2)
        secondaryIndexPersonNotUsingQuery(em2)
		val em3 = serialize(em2)
		primaryIndex(em3)
		secondaryIndexQueriesPerson(em3)
		secondaryIndexQueriesChildren(em3)
		fullscan(em3)
		secondaryIndexPersonNotUsingQuery(em3)
	}
      
      def primaryIndex(em: EntityManager) {
        println("starting to get")
		val startTime = System.currentTimeMillis()
		
		val now = new Date()
		val it = 1000000
		for (i <- 0 until it) {
			val key = Random.nextInt(entitiesToCreate * 2).toLong
			val e = em.get("person", key, now)
		}
		
		val endTime = System.currentTimeMillis()
		println("gets took " + (endTime - startTime))
		println("primary index get avg time " + (endTime - startTime) / (it * 1.0))
      }
      
      def fullscan(em: EntityManager) {
        val t1 = System.nanoTime()
        em.fullScan("person", new Date(), (e:Entity) => e("firstName") == "christian")
        val t2 = System.nanoTime()
        println("fullscan took " + TimeUtil.printNanos(t2-t1))
      }
      
      def serialize(em: EntityManager) : EntityManager = {
        val t1 = System.nanoTime()
        val file = new File("em.tmp")
        val fos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
        val pbem = Serializer.write(em, fos)
        val t2 = System.nanoTime()
        fos.close
        println("serialization time " + TimeUtil.printNanos(t2-t1))
        println("file size " + new File("em.tmp").length / (1024 * 1024) + " mb")
                
        val t3 = System.nanoTime()
        val fis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))
        val em2 = DeSerializer.read(fis)
        val t4 = System.nanoTime()
        fis.close
        println("de-serialization took " + TimeUtil.printNanos(t4-t3))
        em2
      }
      
      def queries() {
    	  val res = Query("entity person where lastName = :1", Array[Any]("Hansen"), em2, new Date())
		  println("query result size " + res.size);
	      
	      val res2 = Query("entity person where lastName = :1", Array[Any]("Petersen"), em2, new Date())
		  println("query result size " + res2.size);
	      
	      val res3 = Query("entity person where lastName = :1", Array[Any]("Jespersen"), em2, new Date())
		  println("query result size " + res3.size);
	      
	      val res4 = Query("entity person where zip = :1", Array[Any](8260), em2, new Date())
		  println("query result size " + res4.size);
	      
	      val res5 = Query("entity person where zip > :1 and zip < :2", Array[Any](7260, 8500), em2, new Date())
		  println("query result size " + res5.size);
	      
	      val res6 = Query("entity person where zip = :1", Array[Any](5690), em2, new Date())
		  println("query result size " + res6.size);
      }
      
      def secondaryIndexQueriesPerson(em: EntityManager) {
        println("making secondary index quesries")
    	val iterations = 10000
        var size = 0;
        val t1 = System.nanoTime()
        for (i <- 0 until iterations) {
        	val lastName = LoadData.sirName
        	val res = Query("entity person where lastName = :1", Array[Any](lastName), em2, new Date())
        	size += res.size
        	//println("person query took " + TimeUtil.printNanos(t2-t1) + " result size " + res.size + " time per id:  " + TimeUtil.printNanos((t2-t1)/size))
        }
    	val t2 = System.nanoTime()
    	println("secondary indexes took "+ TimeUtil.printNanos(t2-t1) + ". Avg result size " + size /iterations + ". avg time per entity " + ((t2-t1)/1000000.0)/size)
      }
      
      def secondaryIndexPersonNotUsingQuery(em: EntityManager) {
        println("making secondary index quesries")
    	val iterations = 10000
        var size = 0;
        val now = new Date()
        val t1 = System.nanoTime()
        val ed = em.get("person")
        val index = ed.indexes("lastName")
        for (i <- 0 until iterations) {
        	val lastName = LoadData.sirName
        	val ids = index === (lastName, now)
        	val res = ids.map(ed.apply(_, em.dataPool))
        	size += res.size
        	//println("person query took " + TimeUtil.printNanos(t2-t1) + " result size " + res.size + " time per id:  " + TimeUtil.printNanos((t2-t1)/size))
        }
    	val t2 = System.nanoTime()
    	println("secondaryIndexPersonNotUsingQuery took "+ TimeUtil.printNanos(t2-t1) + ". Avg result size " + size /iterations + ". avg time per entity " + ((t2-t1)/1000000.0)/size)
      }
      
      def secondaryIndexQueriesChildren(em: EntityManager) {
        val ids = em.get("person").primaryIndex.keys.take(10000)
        var size = 0;
        val t1 = System.nanoTime()
        for(id <- ids) {
          val res = Query("entity children where parent = :1", Array[Any](id), em2, new Date())
          size += res.size
        }
        val t2 = System.nanoTime()
        println("secondary indexes for children took "+ TimeUtil.printNanos(t2-t1) + ". Avg result size " + size /ids.size + ". avg time per entity " + ((t2-t1)/1000000.0)/size)
        
      }
      
	  //println("children: " + em2.map("children").primaryIndex.size)
	  //em2.fullScan("children", new Date(), e => true)
	  //println("children fullscan took " + (System.currentTimeMillis - t) + " millis")
	  
	  
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
  
  var lastTime = System.currentTimeMillis()
  def generate(number: Long, em: EntityManager) : EntityManager = {
    val d = new Date()
    Range.Long(0,number,1).foldLeft(em) {(em, id) =>
      if (id % 100000 == 0) {
          val time = System.currentTimeMillis() - lastTime
          val throughput = 100000.0 / time
    	  println("inserted throughput " + throughput + " per milli (multiply by 3. And add index overhead)")
    	  println("generated " + id + " persons")
    	  MemUtil.printMem()
    	  lastTime = System.currentTimeMillis()
      }
      highestPersonId = id
      val em1 = em.add("person", d, generatePerson(id))
      val em2 =  generateChildrenRelation(id).foldLeft(em1) {(em, e) =>
      	em.add("children", d, e)
      }
      em2
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
       "firstName" -> LoadData.firstName, "middleName" -> LoadData.middleName, "lastName" -> LoadData.sirName,
       "street" -> LoadData.roadName, "streetNumber" -> LoadData.roadNumber,
       "zip" -> zip.zip, "city" -> zip.city 
   )
   m
  }
  
  var childrenKey: Long = 0
  def generateChildrenRelation(id: Long): List[Entity] = {
    if (id > 0) {
    	val mom = Math.abs(Random.nextLong() % id)
    	val dad = Math.abs(Random.nextLong() % id)
    	childrenKey += 2
    	List(Map("id" -> (childrenKey-2), "parent" -> mom, "child" -> id),Map("id" -> (childrenKey-1), "parent" -> dad, "child" -> id))
    } else List()
  }
  
}