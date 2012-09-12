package com.memstore
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.File
import scala.collection.JavaConversions._

object TryOut extends Application{
  val resEm = Loader.loadPricelists(EntityManager(), new File("/Users/chr/ws-scala/pricelist-scala/data/takst"))
  
}