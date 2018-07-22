package com.memstore

import org.scalacheck.Gen
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import com.memstore.entity.EntityConfig
import com.memstore.Types.Entity
import scala.util.Random

object Generators {
  
  def genString : Gen[String] =
    Gen.frequency(1 -> genUTF8, 99 -> Gen.alphaStr)
  
  def genUTF8: Gen[String] = Gen.containerOf[List,Char](Gen.choose(Character.MIN_VALUE,Character.MAX_VALUE)) map {_.filter{c=>Character.isDefined(c) && !Character.isLowSurrogate(c) && !Character.isHighSurrogate(c)}.mkString}
    
  val valueGeneratorForClass =  Map[Class[_], Gen[Any]](classOf[String] -> genString, classOf[Long] -> arbitrary[Long])
  
  
  def genAny: Gen[Any] = {
    val index = Random.nextInt(valueGeneratorForClass.size)
    valueGeneratorForClass.values.toArray.apply(index)
  }
  
  def randomClass: Class[_] = {
    val array = valueGeneratorForClass.keys.toArray
    array(Random.nextInt(array.size))
  }

  
  class UniqueFilter[T] extends Function1[T, Boolean] { 
    val values = scala.collection.mutable.Set[T]() 
    def apply(t: T) = { 
      val rv = values.contains(t) 
      values += t 
      //println("generatng " + t + " " + !rv)
      !rv 
    }
  }
  
  val uniqueFilter = new UniqueFilter[String]()
  
  case class ColumnsGenerator(columns: List[(String, Gen[Any])])
  
  case class EntitiesWithConfig(config: EntityConfig, entities: Seq[Entity])
  
  def genColumns: Gen[ColumnsGenerator] = {
      val columns: Gen[List[(String, Gen[Any])]] = for(
          keys <-  Gen.listOfN(Random.nextInt(30), genString.filter(uniqueFilter));
          key <- keys
      ) yield (key, genAny)
      for(cs <- columns) yield ColumnsGenerator(cs)
  }
  
  def genEntity(cg: ColumnsGenerator): Gen[Entity] = {
    for(
      values <- Gen.sequence[List, Any](cg.columns.map(_._2))
    ) yield {
      val columns = cg.columns.map(_._1)
      val zipped = columns.zip(values)
      Map(zipped:_*)
    }
  }

  def genEntities: Gen[EntitiesWithConfig ]= {
    for(
        columnGen <- genColumns;
        size <- Gen.choose(0,10);
        entities <- Gen.listOfN(size, genEntity(columnGen));
        name <- genString
    ) yield {
    	val index = Random.nextInt(columnGen.columns.size)
    	val key = columnGen.columns.toIndexedSeq(index)._1
    	val ec = EntityConfig(name, key)
      EntitiesWithConfig(ec, entities)
    }
  }
}