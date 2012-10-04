package com.pricelist
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.File
import scala.collection.JavaConversions._
import com.memstore.util.Zip
import com.memstore.entity.EntityManager
import com.memstore.serialization.Serializer
import com.memstore.serialization.DeSerializer
import com.memstore.serialization.Serialization.PBEntityManager
import com.memstore.entity.EntityTimelineWithNoHistory
import com.memstore.entity.impl.cepb.CEPB

object TryOut extends Application{
  val em = Loader.loadPricelists(EntityManager(), new File("/Users/chr/ws-scala/pricelist-scala/data/takst/"), 1)

  em.map.foreach{case (name, ed) => 
  	val sum = ed.primaryIndex.values.foldLeft(0) {(sum, et) => 
  	  sum + et.asInstanceOf[EntityTimelineWithNoHistory].ce.asInstanceOf[CEPB].pbeBytes.size
  	}
  	val avg = sum / ed.primaryIndex.size
  	println("entity avg size " + name + " " + avg + " bytes")
  }
  
  val t = System.currentTimeMillis
  val pbemBytes = Serializer.serialize(em).toByteArray()
  println("serialization took " + (System.currentTimeMillis - t))
  println("zipped size " + Zip.zip(pbemBytes).size)
  val t1 = System.currentTimeMillis
  DeSerializer.deSerialize(PBEntityManager.parseFrom(pbemBytes))
  println("de-serialization took " + (System.currentTimeMillis - t1))
}