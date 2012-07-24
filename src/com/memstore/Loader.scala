package com.memstore

import java.io.File
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.FilenameFilter
import scala.collection.JavaConversions._
import com.memstore.entity.ValuePool
import com.memstore.entity.CompactEntityPool

object Loader {
  
  val filteredEntitites = Set[String]("Takst", "ATCKoderOgTekst", "Doseringskode", "Indholdsstoffer", "Indikationskode", "LaegemiddelAdministrationsvejRef",
      "Pakningskombinationer", "Pakningsstoerrelsesenhed", "Styrkeenhed", "Tilskudsintervaller", "UdgaaedeNavne")

  def loadPricelists(rootDir: File) : EntityManager = {
	  val pricelistDirs = rootDir.listFiles(new FilenameFilter() {
		  def accept(file: File, name: String) = file.isDirectory && !name.startsWith(".")
	  })

	  var counter = 1
	  pricelistDirs.foldLeft(EntityManager()){(em, dir) =>
	  println("loading pricelist " + counter)
	  val em1 = loadPricelist(dir, em) 
	  checkTimelines(em1)
	  counter += 1
	  printMem()
	  printPools()
	  em1
	  }
  }

	def loadPricelist(dir: File, em: EntityManager) : EntityManager = {
		val pricelistElems = TakstImporter.importTakst(dir).getDatasets()
		pricelistElems.foldLeft(em) {
		  (em, elements) => 
		    val date = elements.getValidFrom().getTime()
		    ObjectParser(elements.getEntities().toSet) match {
		      case (name, entities) => {
		    	  if (entities.isEmpty || filter(name)) {
		    		  em
		    	  } else {
		    	    entities.foldLeft(em) { (em, e) =>
		    	      em.add(name, date, e)
		    	    }
		    	  }
		      }
		    }
		}
	}
	
	private def filter(name: String): Boolean = {
	  val remove = filteredEntitites.contains(name)
	  if (remove) println("not loading " + name)
	  remove
	}
	
	private def checkTimelines(em: EntityManager) {
	  var acc = Vector[Int]()
	  for(entityData <- em.map.values;
	      et <- entityData.primaryIndex.values
	  ) acc = acc :+ et.timeline.length
	  val sorted = acc.sorted
	  println("timeline length stats")
	  val length = sorted.length
	  println("total timelines: " + length)
	  println("timeline length 50 percentile: " + sorted((length * 0.5).toInt))
	  println("timeline length 75 percentile: " + sorted((length * 0.75).toInt))
	  println("timeline length 90 percentile: " + sorted((length * 0.90).toInt))
	  println("timeline length 99 percentile: " + sorted((length * 0.99).toInt))
	  println("timeline length 99.9 percentile: " + sorted((length * 0.999).toInt))
	  println("timeline length 99.99 percentile: " + sorted((length * 0.9999).toInt))
	  println("timeline length 99.999 percentile: " + sorted((length * 0.99999).toInt))
	  println("timeline length max: " + sorted.last)
	}
	
	private def printMem() {
		System.gc();
		val r = Runtime.getRuntime();
		val mem = (r.totalMemory() - r.freeMemory()) / (1024*1024); //megabytes
		println("used mem: " + mem);
	}
	
	var lastCePoolSize = 0;
	var lastValuePoolSize = 0;
	private def printPools() {
	  val newValueSize = ValuePool.allValues.size
	  val newCeSize = CompactEntityPool.allEntities.size
	  println("value pool size " + newValueSize)
	  println("value pool grown " + (newValueSize - lastValuePoolSize))
	  println("ce pool size " + newCeSize)
	  println("ce pool keysize grown " + (newCeSize - lastCePoolSize))
	  lastCePoolSize = newCeSize
	  lastValuePoolSize = newValueSize
	}
} 