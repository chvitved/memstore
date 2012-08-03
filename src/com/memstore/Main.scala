package com.memstore

import java.io.File
import com.memstore.Types.Entity

object Main extends Application{
  
  val drugNameIndex = new IndexConfig("drugname", (e: Entity) => e("navn").asInstanceOf[String])
  val em = EntityManager().addEntity(new EntityConfig("Laegemiddel", drugNameIndex))
  val resEm = Loader.loadPricelists(em, new File("/Users/chr/ws-scala/pricelist-scala/data/takst"))

}