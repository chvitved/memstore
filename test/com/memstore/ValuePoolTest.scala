package com.memstore
import org.junit.Test
import org.junit.Assert._

class ValuePoolTest {
  
  @Test
  def testWithSameNumberIndifferentTypes() {
    val int20: java.lang.Integer = 20
    val long20: java.lang.Long = 20L
    val v2: java.lang.Integer = ValuePool.intern(int20)
    val v1: java.lang.Long = ValuePool.intern(long20)
    
    
  }

}