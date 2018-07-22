package com.memstore.entity

import org.junit.Test
import java.util.Date
import org.junit.Assert._
import org.junit.Before
import com.memstore.Types.Entity

class HistoryTest {
  
  @Test
  def testTemporalDataWithNulls() {
    val e1 = create(("id" -> "id1"), ("a" -> null))
    val e2 = create(("id" -> "id1"), ("a" -> null), ("b" -> "world"))
    testEntityHistory(e1, e2)
  }
  
  
  @Test
  def testColumnchange() {
    val e1 = create(("id" -> 1), ("a" -> 1))
    val e2 = create(("id" -> 1), ("a" -> 2))
    testEntityHistory(e1, e2)
  }
  
  @Test
  def testColumnAdded() {
	val e1 = create(("id" -> "id1"))
	val e2 = create(("id" -> "id1"), ("a" -> 5))
	testEntityHistory(e1, e2)
  }
  
  @Test
  def testColumnremove() {
    val e1 = create(("id" -> 1), ("a" -> 1))
    val e2 = create(("id" -> 1))
    testEntityHistory(e1, e2)
  }
  
  
  def create(tuples: (String, Any)*): Entity = {
    Map[String, Any](tuples:_*)
  }
  
  private def testEntityHistory(e1: Entity, e2: Entity) {
    val notPooledKeys = e1.keySet ++ e2.keySet
    val edWithoutPooling = EntityData(EntityConfig("test", notPooledKeys.toSeq, Seq()))
    doTestEntities(e1, e2, edWithoutPooling)
    
    val ed = EntityData(EntityConfig("test"))
    doTestEntities(e1, e2, ed)
  }
  
  private def doTestEntities(e1: Entity, e2: Entity, ed: EntityData) {
    val t1 = new Date(1000L) 
    val t2 = new Date(2000L)
    val now = new Date()
    
    val p1 = CompactEntity.emptyDataPool
    
    val (ed2, p2) = ed + (t1, e1, p1)
    val (ed3, p3) = ed2 + (t2, e2, p2)
    
    val fetch1 = ed3(e1("id"), t1, p3)
    val fetch2 = ed3(e2("id"), t2, p3)
    
    assertEquals(Some(e1.filter(_._2 != null)), fetch1)
    assertEquals(Some(e2.filter(_._2 != null)), fetch2)
  }

}