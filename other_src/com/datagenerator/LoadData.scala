package com.datagenerator

import scala.io.Source
import scala.util.Random

object LoadData {
  
  private val firstNames = loadNames("datafiles/firstnames.txt")
  private val middleNames = loadNamesWithNulls("datafiles/sirnames.txt", 0.2)
  private val sirnames = loadNames("datafiles/middlenames.txt")
  private val roads = Source.fromFile("datafiles/out/roads.txt").getLines().toArray 
  private val zipCodes: Array[ZipCode] = Source.fromFile("datafiles/zipcodes.txt").getLines().map{l => 
      if (l.contains("-") && l.split("  ").first.contains("-")) {
        val numberAndCity = l.split("  ")
        val numbers = numberAndCity(0).split("-")
        val city = numberAndCity(1)
        val numberArray = Range(numbers(0).toInt, numbers(1).toInt)
        numberArray.map(number => ZipCode(number, city)).toArray[ZipCode]
      } else {
    	  val split = l.split("  ")
    	  Array[ZipCode](ZipCode(split(0).toInt, split(1)))
      }
    }.toArray.flatten
    
    
  def firstName = random(firstNames)
  def middleName = random(middleNames)
  def sirName = random(sirnames)
  def roadName = random(roads)
  def zipCode = random(zipCodes)
  
  
  def roadNumber : Int = {
    scala.util.Random.nextInt(10000)
  }
  
  private def loadNamesWithNulls(file: String, nullPercentage: Double): Array[String] = {
    val names = loadNames(file)
    val numberOfNulls = names.length * nullPercentage
    val nulls: Seq[String] = for(i <- 1 to numberOfNulls.toInt) yield null 
    names ++ nulls.toArray[String]
  }
  
  private def random[T](array: Array[T]): T = {
    array(scala.util.Random.nextInt(array.length))
  }
  
  private def loadNames(file: String): Array[String] = {
    Source.fromFile(file, "UTF-8").getLines().map{l =>l.split("\t")(2)}.map{l =>
      if (l.contains(",")) {
        l.split(",")
      } else {
    	  Array[String](l)
      }
    }.toArray.flatten
  }

}