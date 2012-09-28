package com.pricelist
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.File
import scala.collection.JavaConversions._
import com.memstore.util.Zip
import com.memstore.entity.EntityManager
import com.memstore.serialization.Serializer
import com.memstore.serialization.DeSerializer
import com.memstore.serialization.Serialization.PBEntityManager

object TryOut extends Application{
  val em = Loader.loadPricelists(EntityManager(), new File("/Users/chr/ws-scala/pricelist-scala/data/takst/"), 1)

  val t = System.currentTimeMillis
  val pbemBytes = Serializer.serialize(em).toByteArray()
  println("serialization took " + (System.currentTimeMillis - t))
  println("zipped size " + Zip.zip(pbemBytes).size)
  val t1 = System.currentTimeMillis
  DeSerializer.deSerialize(PBEntityManager.parseFrom(pbemBytes))
  println("de-serialization took " + (System.currentTimeMillis - t1))
}