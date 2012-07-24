package com.memstore
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.File
import scala.collection.JavaConversions._

object TryOut extends Application{
  val pricelistElems =TakstImporter.importTakst(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20100101")).getDatasets()
  val elements = pricelistElems.foldLeft(0){(acc, e )=>
    acc + e.getEntities().size()
  }
  println("elements in pricelist " + elements)
  
}