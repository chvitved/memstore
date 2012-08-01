package com.memstore

import java.io.File
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.FilenameFilter
import scala.collection.JavaConversions._

object Loader {
  
  val filteredEntitites = Set[String]("Takst", "Tidsenhed", "Pakningsstoerrelsesenhed", "Styrkeenhed")

  def loadPricelists(em: EntityManager, rootDir: File) : EntityManager = {
	  val pricelistDirs = rootDir.listFiles(new FilenameFilter() {
		  def accept(file: File, name: String) = file.isDirectory && !name.startsWith(".")
	  })

	  var counter = 1
	  pricelistDirs.foldLeft(em){(em, dir) =>
		  println("loading pricelist " + counter)
		  val em1 = loadPricelist(dir, em) 
		  checkTimelines(em1)
		  counter += 1
		  printMem()
		  printPools()
		  Monitor.showAndClear()
		  em1
	  }
  }

	def loadPricelist(dir: File, em: EntityManager) : EntityManager = {
	  	val t1 = System.currentTimeMillis()
		val pricelistElems = TakstImporter.importTakst(dir).getDatasets()
		val t2 = System.currentTimeMillis()
		val newEm = pricelistElems.foldLeft(em) {
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
	  	val t3 = System.currentTimeMillis()
	  	println("time loading pricelist " + (t3-t1))
	  	println("time parsing pricelist with sdm module " + (t2-t1))
	  	println("time inserting in memstore " + (t3-t2))
	  	newEm
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
	  if (length > 0) {
		  println("timeline length 50 percentile: " + sorted((length * 0.5).toInt))
		  println("timeline length 75 percentile: " + sorted((length * 0.75).toInt))
		  println("timeline length 90 percentile: " + sorted((length * 0.90).toInt))
		  println("timeline length 99 percentile: " + sorted((length * 0.99).toInt))
		  println("timeline length 99.9 percentile: " + sorted((length * 0.999).toInt))
		  println("timeline length 99.99 percentile: " + sorted((length * 0.9999).toInt))
		  println("timeline length 99.999 percentile: " + sorted((length * 0.99999).toInt))
		  println("timeline length max: " + sorted.last)
	  }
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
	  val newValueSize = ValuePool.size
	  println("value pool size " + newValueSize)
	  println("value pool grown " + (newValueSize - lastValuePoolSize))
	  lastValuePoolSize = newValueSize
	}
} 