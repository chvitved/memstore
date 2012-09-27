package com.memstore.entity
import java.util.Arrays
import com.memstore.Types.Entity
import com.memstore.entity.impl.cepb.{CEPBMetaData, CEPB}

object CompactEntity {
	
	def apply(entity: Entity, metaData: CompactEntityMetaData): (CompactEntity, CompactEntityMetaData) = {
	  //impl.cebitmap.CEBitmapImpl(entityName, entity)
	  CEPB(entity, metaData)
	}
	
	def emptyMetaData(name: String, notPooledcolumns: Seq[String]): CompactEntityMetaData = CEPBMetaData(name, notPooledcolumns) 
	
}
trait CompactEntity {
  
  def get(metaData: CompactEntityMetaData) : Entity
  
  def getValue(attribute: String, metaData: CompactEntityMetaData) = get(metaData)(attribute)
  
}
