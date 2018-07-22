package com.pricelist

import java.io.File
import com.memstore.Types.Entity
import java.util.Date
import com.memstore.entity.EntityManager
import com.memstore.entity.EntityConfig
import com.memstore.index.Index
import com.memstore.query.Query
import com.memstore.util.TimeUtil
import java.util.concurrent.Executors
import java.util.zip.GZIPInputStream
import com.memstore.serialization.DeSerializer
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.util.zip.GZIPOutputStream
import com.memstore.serialization.Serializer
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import com.datagenerator.MemUtil

object Main extends Application{
  
  //val loadedEm = loadEm("6mio_persons.dump")
  val loadedEm = EntityManager()
  val em = loadedEm.addEntity(EntityConfig("Laegemiddel", List[String](), List("navn", "aTC")))
  val em1 = em.addEntity(EntityConfig("Pakning", List[String](), List("drugid")))
  
  
  val resEm = Loader.loadPricelists(em1, new File("/Users/chr/data"))
  //val (e1, previous) = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20110101"), em1, null)
  //val (e2, prev) = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20110110"), e1, previous)
  //val (e3, pr) = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20110124"), e2, prev)
  //val (e4, p) = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20110207"), e3, pr)
  
  val em2 = resEm
  
  println(em2.get("Pakning").primaryIndex.size + " pakninger")
  println(em2.get("Laegemiddel").primaryIndex.size + " l¾gemidler")
  
  saveEm(em2, "pricelists.dump")
  
  //wait("take snapshot", 10000)
  
//  query("entity Laegemiddel where aTC = :1", Array("C10AA01"), em2)
//  query("entity Laegemiddel where aTC = :1", Array("N02AX02"), em2)
//  query("entity Laegemiddel where aTC = :1", Array("R03BA02"), em2)
//  query("entity Laegemiddel where aTC = :1", Array("N05AX08"), em2)
//  query("entity Laegemiddel where aTC = :1", Array("N03AX09"), em2)
//  
//  query("entity Pakning where drugid = :1", Array(28100789776L), em2)
//  query("entity Pakning where drugid = :1", Array(28103711904L), em2)
//  query("entity Pakning where drugid = :1", Array(28101783895L), em2)
//  query("entity Pakning where drugid = :1", Array(28101907097L), em2)
//  
//  val m = ()=> {
//    val ed = em2.get("Pakning")
//    println("primary index...")
//    time(() => {
//      ed(590497L, em2.dataPool)
//    })
//    
//    
//    val t1 = System.nanoTime()
//    val r = ed.indexes("drugid").===(28103334301L, new Date())
//    println("index took " + TimeUtil.printNanos(System.nanoTime() - t1))
//    println("ids size " + r.size)
//    
//    val res = r
//    val pool = Executors.newFixedThreadPool(2)
//    pool.submit(new Runnable() {
//      def run() = println("hello")
//    })
//    Thread.sleep(1000)
//    
//    time(() => {
//      val t1 = System.nanoTime()
//      for(id <- res) {
//        pool.submit(new Runnable() {
//          def run() = ed(id, em2.dataPool)
//        })
//      }
//      var t2 = System.nanoTime();
//      pool.submit(new Runnable() {
//    	  def run() = {t2 = System.nanoTime()}
//      })
//      Thread.sleep(1000)
//      println("strange test " + TimeUtil.printNanos(t2 - t1))
//      //res.flatMap{id => ed(id, em2.dataPool)}
//    })
//    time(() => {
//      res.flatMap{id => ed(id, em2.dataPool)}
//    })
//  }
//  
//  time(m)
//  
//  for(i <- 0 until 1) new Thread() {
//    override def run() {runPrimaryIndexTest(em2)}
//  }.start
  //runSecondaryIndexTest(em2)
  //new Thread() {override def run() {runQueryTest(em2)}}.start
  //new Thread() {override def run() {runPrimaryIndexTest(em2)}}.start
  
//  runQueryTest(em2)
//  runQueryTest(em2)
//  runQueryTest(em2)
//  runQueryTest(em2)
//  runQueryTest(em2)
//  runQueryTest(em2)
//  runQueryTest(em2)
//  runQueryTest(em2)
//  runQueryTest(em2)
//  
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runPrimaryIndexTest(em2)
//  runQueryTest(em2)
  

  
//  query("entity Pakning where id = :1", Array(520346L), em2)
//  query("entity Pakning where id = :1", Array(420356L), em2)
//  time(() => em2.get("Pakning",520346L, new Date()))
//  time(() => em2.get("Pakning",420356L, new Date()))
  
  
  //wait("done", 30000)
  
  
  def loadEm(filename: String): EntityManager = {
    val t1 = System.nanoTime()
    val file = new File(filename)
    val fis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))
    val em = DeSerializer.read(fis)
    val t2 = System.nanoTime()
    fis.close
    println("deserialization time " + TimeUtil.printNanos(t2-t1))
    println("file size " + file.length / (1024 * 1024) + " mb")
    MemUtil.printMem()
    em
  }
  
  def saveEm(em: EntityManager, fileName: String) {
    val file = new File(fileName)
    val fos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
    val pbem = Serializer.write(em, fos)
    fos.close
  }
  
  def wait(msg: String, time: Int) {
    println(msg)
    Thread.sleep(time)
  }
  
  def query(query: String, params: IndexedSeq[Any],em: EntityManager) {
    def q = () => {
      val res = Query(query, params, em, new Date())
      println("size: " + res.size)
    }
    time(q)
    println()
  }
  
  def time(method: ()=>Any) {
    val time = System.nanoTime();
    method()
    println("time " + TimeUtil.printNanos(System.nanoTime() - time))
  }
  
  def runQueryTest(em: EntityManager) {
    val ed = em.get("Pakning")
    val entities = em.fullScan("Pakning", new Date(), (e) => true)
    val drugIds = entities.map(_("drugid"))
    var size = 0;
    val t1 = System.nanoTime()
    for(drugid <- drugIds) {
      val res = Query("entity Pakning where drugid = :1", Array(drugid), em, new Date())
      size = size + res.size
    } 
    val t2 = System.nanoTime()
    println("queries took " + TimeUtil.printNanos(t2-t1) + " result size " + size + " time per id:  " + TimeUtil.printNanos((t2-t1)/size))
  }
  
  def runPrimaryIndexTest(em: EntityManager) {
    val ed = em.get("Laegemiddel")
    val entities = em.fullScan("Laegemiddel", new Date(), (e) => true)
    val ids = entities.map(_("id"))
    val t1 = System.nanoTime()
    ids.map(ed(_, em.dataPool))
    val t2 = System.nanoTime()
    println("primary indexes took " + TimeUtil.printNanos(t2-t1) + ". time per id:  " + TimeUtil.printNanos((t2-t1)/ids.size))
  }
  
  def runSecondaryIndexTest(em: EntityManager) {
    val ed = em.get("Pakning")
    val entities = em.fullScan("Pakning", new Date(), (e) => true)
    val drugIds = entities.map(_("drugid"))
    
    for(drugId <- drugIds) {
    	val t1 = System.nanoTime()
    	val ids = ed.indexes("drugid") === (drugId, new Date())
    	val t2 = System.nanoTime()
    	val res = ids.map(ed(_, em.dataPool))
    	val t3 = System.nanoTime()
    	println("secondary index took " + TimeUtil.printNanos(t2-t1) + " found " + ids.size)
    	println("ids took " + TimeUtil.printNanos((t3-t2) / res.size))
    	println("query found " + res.size + " results in " + TimeUtil.printNanos(t3-t1) + " per result " + TimeUtil.printNanos((t3-t1) / res.size))
    	println()
    }
  }

//  val index = em2.get("Laegemiddel").indexes("drugname")
//  val metaData = em2.get("Laegemiddel").metaData
//  val pool = em2.dataPool
//  
//  for(i <- 0 until 3) doStuff()
//  
//  private def doStuff() {
//	  {
//	  val from = "A"
//	  val to = "B"
//	  val t1 = System.currentTimeMillis();
//	  val res = index.range(from, to, new Date(), metaData, pool)
//	  println("Time " + (System.currentTimeMillis() - t1))
//	  println("results " + res.size)
//	  //val names = res.toList.map(_("navn").toString).toList.sorted
//	  //names.foreach(println(_))
//	  }
//	  
//	  {
//	  val from = "B"
//	  val to = "C"
//	  val t1 = System.currentTimeMillis();
//	  val res = index.range(from, to, new Date(), metaData, pool)
//	  println("Time " + (System.currentTimeMillis() - t1))
//	  println("results " + res.size)
//	  //val names = res.toList.map(_("navn").toString).toList.sorted
//	  //names.foreach(println(_))
//	  }
//	  
//	  {
//	  val from = "Pan"
//	  val to = "Pao"
//	  val t1 = System.currentTimeMillis();
//	  val res = index.range(from, to, new Date(), metaData, pool)
//	  println("Time " + (System.currentTimeMillis() - t1))
//	  println("results " + res.size)
//	  //val names = res.toList.map(_("navn").toString).toList.sorted
//	  //names.foreach(println(_))
//	  }
//	  
//	  val keyTime = System.currentTimeMillis()
//	  
//	  {
//	  val t2 = System.currentTimeMillis();
//	  val res1 = em2.get("Pakning", 520338L, new Date())
//	  println("Time " + (System.currentTimeMillis() - t2))
//	  }
//	  
//	  {
//	  val t2 = System.currentTimeMillis();
//	  val res1 = em2.get("Pakning", 420356L, new Date())
//	  println("Time " + (System.currentTimeMillis() - t2))
//	  }
//	  
//	  {
//	  val t2 = System.currentTimeMillis();
//	  val res1 = em2.get("Pakning", 504902L, new Date())
//	  println("Time " + (System.currentTimeMillis() - t2))
//	  }
//	  
//	  println("keyTime " + (System.currentTimeMillis() - keyTime))
//	  
//	  {
//	  //scan
//	  val predicate = (e: Entity) => e("navn").asInstanceOf[String].contains("em") 
//	  val t2 = System.currentTimeMillis();
//	  val res = em2.fullScan("Laegemiddel", new Date(), predicate)
//	  println("fullscan l¾gemidler time " + (System.currentTimeMillis() - t2))
//	  println(res.size)
//	  println(res.flatMap(_.get("navn")))
//	  }
//	  
//	  {
//	  //scan
//	  val predicate = (e: Entity) => true
//	  val t2 = System.currentTimeMillis();
//	  val res = em2.fullScan("Pakning", new Date(), predicate)
//	  println("fullscan pakninger time " + (System.currentTimeMillis() - t2))
//	  println(res.size)
//	  }
// }
}