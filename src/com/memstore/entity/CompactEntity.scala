package com.memstore.entity
import java.util.Arrays
import com.memstore.Types.Entity
import com.memstore.entity.impl.cepb.{CEPBMetaData, CEPB}
import com.memstore.entity.impl.cepb.CEPBDataPool

object CompactEntity {
	
	def apply(entity: Entity, metaData: CompactEntityMetaData, pool: CompactEntityDataPool): (CompactEntity, CompactEntityMetaData, CompactEntityDataPool) = {
	  //impl.cebitmap.CEBitmapImpl(entityName, entity)
	  CEPB(entity, metaData, pool)
	}
	
	def emptyMetaData(name: String, notPooledcolumns: Seq[String]): CompactEntityMetaData = CEPBMetaData(name, notPooledcolumns) 
	
	def emptyDataPool : CompactEntityDataPool = CEPBDataPool(Map())
	
}
trait CompactEntity {
  
  def get(metaData: CompactEntityMetaData, pool: CompactEntityDataPool) : Entity
  
  def getValue(attribute: String, metaData: CompactEntityMetaData, pool: CompactEntityDataPool) = get(metaData, pool)(attribute)
  
}
