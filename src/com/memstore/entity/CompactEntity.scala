package com.memstore.entity
import java.util.Arrays
import com.memstore.Types.Entity
import com.memstore.entity.impl.cebitmap.CEBitmapImpl

object CompactEntity {
	
	def apply(entityName: String, entity: Entity): CompactEntity = {
	  CEBitmapImpl(entityName, entity)
	}
}
trait CompactEntity {
  
  def get : Entity
  
  def getValue(attribute: String) = get(attribute)
  
}
