package com.memstore

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Arbitrary
import com.memstore.Types.Entity
import com.memstore.entity.EntityManager
import java.util.Date
import com.memstore.Generators.EntitiesWithConfig

object RandomTest extends Properties("memstore") {
  
  implicit def entityconfig = Arbitrary(Generators.genColumns)
  implicit def entities = Arbitrary(Generators.genEntities)
  implicit def test = Arbitrary(Generators.genAny)
  
  
  property("add") = forAll{(ecs: EntitiesWithConfig) => {
	  val (config, entities) = (ecs.config, filterDuplicateKeys(ecs.config.key, ecs.entities))
	  val em = entities.foldLeft(EntityManager().addEntity(config)) {(em, e) => 
	    em add (config.name, new Date(), e)
	  }
	  
	  val entityName = ecs.config.name
	  val key = ecs.config.key
	  
	  entities.forall(e => Some(e) == em.get(entityName, key))
	  
  }}
  
  private def filterDuplicateKeys(key: String, entities: Seq[Entity]): Seq[Entity] = {
    val map = entities.foldLeft(Map[Any, Entity]()) {(map, e) => 
      val keyValue = e(key)
      if (map.contains(keyValue)) {
        map
      } else map + (keyValue -> e)
    }
    map.values.toSeq
  }
  
  
//  
//  property("config-temp") = forAll{(e: Entity) => { 
//    println("--")
//    println(e)
//    true
//  }}
  
//   property("config-temp") = forAll{(es: Seq[Entity]) => { 
//    println("--")
//    es.foreach(e => println(e.keys.toList.sorted.map("'" + _ + "'")))
//    true
//  }}
  
//  property("test-temp") = forAll{(o: Any) => { 
//    println("--")
//    println(o)
//    true
//  }}

}