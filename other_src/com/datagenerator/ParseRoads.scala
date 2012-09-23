package com.datagenerator

import java.io.BufferedWriter
import java.io.FileWriter
import java.util.regex.Pattern

import scala.io.Source

object ParseRoads extends App {
  
  //Source.fromFile("datafiles/veje.txt").getLines().filter(_.startsWith("001")).foreach(l => println(l.substring(71, math.min(111, l.length()))))

  var set = Set[String]()
  Source.fromFile("datafiles/zipcodes.txt").getLines().foreach(l => fetchZipCode(l.substring(0,4).toInt))
  
  val string = set.reduceLeft { (acc, n) =>
	acc + "\n" + n
  }

  val w = new BufferedWriter(new FileWriter("datafiles/out/roads.txt"))
  w.write(string)
  w.close
  
  def fetchZipCode(zipCode: Int) {
    def parseString(s : String) = {
      val pattern1 = Pattern.compile("<vejnavn.*?>(.*?)</vejnavn>")
      val pattern2 = Pattern.compile("<navn>(.*?)</navn>")
      val m1 = pattern1.matcher(s)
      while (m1.find()) {
        val value = m1.group(1);
        val m = pattern2.matcher(value)
        if (m.find && m.groupCount() > 0) {
        	val roadName = m.group(1)
        	if (!set.contains(roadName)) {
        		set = set + roadName.intern
        		println(roadName)
        	}
        }
      }
    }
    val o = Source.fromURL("http://geo.oiorest.dk/adresser?postnr=" + zipCode, "UTF-8")
    parseString(o.mkString)
    println("zip " + zipCode)
    println("size " + set.size)
    printMem
  }
  
  private def printMem() {
	System.gc();
	val r = Runtime.getRuntime();
	val mem = (r.totalMemory() - r.freeMemory()) / (1024*1024); //megabytes
	println("used mem: " + mem);
  }

}