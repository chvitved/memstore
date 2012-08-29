package com.memstore

import java.io.File
import com.memstore.Types.Entity
import java.util.Date

object Main extends Application{
  
  val drugNameIndex = new IndexConfig("drugname", (e: Entity) => e("navn").asInstanceOf[String])
  val drugIdIndexConfig = new IndexConfig("drugid", (e: Entity) => e("id").asInstanceOf[Long])
  val em = EntityManager().addEntity(new EntityConfig("Laegemiddel","id", drugNameIndex, drugIdIndexConfig))
  
  
  //val resEm = Loader.loadPricelists(em, new File("/Users/chr/ws-scala/pricelist-scala/data/takst"))
  val (em1, previous) = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20080101"), em, null)
  //val (em2, prev) = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20080101"), em1, previous)
  val em2 = em1
  
  println(em2.get("Pakning").primaryIndex.size + " pakninger")
  println(em2.get("Laegemiddel").primaryIndex.size + " l¾gemidler")

  val drugIdIndex = em2.get("Laegemiddel").indexes("drugid")
  println("found drugid size " + (drugIdIndex < (28103666305L, new Date())).size) 

  
  val index = em2.get("Laegemiddel").indexes("drugname")
  
  {
  val from = "A"
  val to = "B"
  val t1 = System.currentTimeMillis();
  val res = index.range(from, to, new Date())
  println("Time " + (System.currentTimeMillis() - t1))
  println("results " + res.size)
  //val names = res.toList.map(_("navn").toString).toList.sorted
  //names.foreach(println(_))
  }
  
  {
  val from = "B"
  val to = "C"
  val t1 = System.currentTimeMillis();
  val res = index.range(from, to, new Date())
  println("Time " + (System.currentTimeMillis() - t1))
  println("results " + res.size)
  //val names = res.toList.map(_("navn").toString).toList.sorted
  //names.foreach(println(_))
  }
  
  {
  val from = "Pan"
  val to = "Pao"
  val t1 = System.currentTimeMillis();
  val res = index.range(from, to, new Date())
  println("Time " + (System.currentTimeMillis() - t1))
  println("results " + res.size)
  //val names = res.toList.map(_("navn").toString).toList.sorted
  //names.foreach(println(_))
  }
  
  val keyTime = System.currentTimeMillis()
  
  {
  val t2 = System.currentTimeMillis();
  val res1 = em2.get("Pakning", 520338L, new Date())
  println("Time " + (System.currentTimeMillis() - t2))
  }
  
  {
  val t2 = System.currentTimeMillis();
  val res1 = em2.get("Pakning", 420356L, new Date())
  println("Time " + (System.currentTimeMillis() - t2))
  }
  
  {
  val t2 = System.currentTimeMillis();
  val res1 = em2.get("Pakning", 504902L, new Date())
  println("Time " + (System.currentTimeMillis() - t2))
  }
  
  println("keyTime " + (System.currentTimeMillis() - keyTime))
  
  {
  //scan
  val predicate = (e: Entity) => e("navn").asInstanceOf[String].contains("em") 
  val t2 = System.currentTimeMillis();
  val res = em2.fullScan("Laegemiddel", new Date(), predicate)
  println("fullscan time " + (System.currentTimeMillis() - t2))
  println(res.size)
  println(res.flatMap(_.get("navn")))
  }
  
  {
  //scan
  val predicate = (e: Entity) => true 
  val t2 = System.currentTimeMillis();
  val res = em2.fullScan("Pakning", new Date(), predicate)
  println("fullscan time " + (System.currentTimeMillis() - t2))
  println(res.size)
  }
}