package com.memstore
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.File
import scala.collection.JavaConversions._
import com.memstore.serialization.Serializer

object TryOut extends Application{
  val (em1, previous) = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20080101"), EntityManager(), null)
  Serializer.serialize(em1)
}