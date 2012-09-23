package com.pricelist
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.File
import scala.collection.JavaConversions._
import com.memstore.util.Zip
import com.memstore.entity.EntityManager
import com.memstore.serialization.Serializer
import com.memstore.serialization.DeSerializer

object TryOut extends Application{
  val em = Loader.loadPricelists(EntityManager(), new File("/Users/chr/ws-scala/pricelist-scala/data/takst/"), 50)

  val t = System.currentTimeMillis
  val pbem = Serializer.serialize(em)
  println("serialization took " + (System.currentTimeMillis - t))
  println("zipped size " + Zip.zip(pbem.toByteArray()).size)
  val t1 = System.currentTimeMillis
  DeSerializer.deSerialize(pbem)
  println("de-serialization took " + (System.currentTimeMillis - t1))
}