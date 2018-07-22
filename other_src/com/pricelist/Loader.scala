package com.pricelist

import java.io.File
import dk.trifork.sdm.importer.takst.TakstImporter
import java.io.FilenameFilter
import scala.collection.JavaConversions._
import com.memstore.entity.EntityTimeline
import com.memstore.Types.Entity
import dk.trifork.sdm.model.CompleteDataset
import java.util.Date
import com.memstore.entity.EntityManager
import com.memstore.Monitor

object Loader {
  
  val filteredEntitites = Set[String]("Takst", "Tidsenhed", "Pakningsstoerrelsesenhed", "Styrkeenhed")

  def loadPricelists(em: EntityManager, rootDir: File) : EntityManager = {
    loadPricelists(em, rootDir, Integer.MAX_VALUE)
  }
  
  def loadPricelists(em: EntityManager, rootDir: File, max: Int) : EntityManager = {
	  val pricelistDirs = rootDir.listFiles(new FilenameFilter() {
		  def accept(file: File, name: String) = file.isDirectory && !name.startsWith(".")
	  }).take(max)

	  var counter = 1
	  var previous: Seq[CompleteDataset[_]] = null
	  pricelistDirs.foldLeft(em){(em, dir) =>
		  println("loading pricelist " + counter)
		  val (em1, entities) = loadPricelist(dir, em, previous)
		  previous = entities
		  //checkTimelines(em1)
		  counter += 1
		  printMem()
		  Monitor.showAndClear()
		  em1
	  }
  }

	def loadPricelist(dir: File, em: EntityManager, previous: Seq[CompleteDataset[_]]) : (EntityManager, Seq[CompleteDataset[_]]) = {
	  	val t1 = System.currentTimeMillis()
		val pricelistElems = TakstImporter.importTakst(dir).getDatasets()
		val t2 = System.currentTimeMillis()

		val p = if (previous == null) {
				for(i <- 0 until pricelistElems.size) yield null
			} else previous
		
		if (pricelistElems.size != p.size) throw new Exception(String.format("previous and current pricelist does not contain the same elements. current: %s, previous %s", pricelistElems.size + "", previous + ""))
		val zippedElemes = pricelistElems.zip(p)
		
		val em1 = zippedElemes.foldLeft(em) {
		  (em, zippedElemes) => 
		    val elements = zippedElemes._1
		    val previousElements = zippedElemes._2
		    val (name, entities) = ObjectParser(elements.getEntities().toSet)
		    if (entities.isEmpty || filter(name)) {
		    	em
		    } else {
		    	val oldParsedElements = 
		    		if (previousElements == null) Set[Entity]() 
		    		else ObjectParser(previousElements.getEntities().toSet)._2 
    	     	val date = elements.getValidFrom().getTime()
		    	updateStore(oldParsedElements, entities, date, name, em)
		    }
		}
	  	val t3 = System.currentTimeMillis()
	  	println("time loading pricelist " + (t3-t1))
	  	println("time parsing pricelist with sdm module " + (t2-t1))
	  	println("time inserting in memstore " + (t3-t2))
	  	(em1, pricelistElems)
	}
	
	private def updateStore(old: Set[Entity], nev: Set[Entity], date: Date, entityName: String, em: EntityManager): EntityManager = {
	  //delete
	  val keysToDelete = old.map(_("id")) -- nev.map(_("id"))
	  val em1 = keysToDelete.foldLeft(em) {(em, key) =>
	    	em.remove(entityName, date, key)
	  }
	  
	  //insert
	  nev.foldLeft(em1) {(em, e) =>
	  if (!old.contains(e)) {
	    try {
	    	em.add(entityName, date, e)
	    } catch {
	      case e => {
	    	println("got exception inserting into database")  
	        e.printStackTrace()
	        em
	      }
	    }
	  }
	  else em
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
} 