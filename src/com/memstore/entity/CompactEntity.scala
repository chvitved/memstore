package com.memstore.entity
import java.util.Arrays
import com.memstore.Types.Entity

object CompactEntity {
	
	def apply(entityName: String, entity: Entity): CompactEntity = {
	  //impl.cebitmap.CEBitmapImpl(entityName, entity)
	  impl.cepb.CEPB(entityName, entity)
	}
	
}
trait CompactEntity {
  
  def get : Entity
  
  def getValue(attribute: String) = get(attribute)
  
}
