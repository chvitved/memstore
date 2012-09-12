package com.memstore.entity.impl.cebitmap

import java.util.Arrays
import com.memstore.ValuePool
import com.memstore.Types.Entity
import com.memstore.entity.CompactEntity

object CEBitmapImpl {
	
	def apply(entityName: String, entity: Entity): CEBitmapImpl = {
	  validate(entityName, entity)
	  val indexValueTuples = CEBitmapMetaData.getAndUpdate(entityName, entity)
      createCompactEntity(entityName, indexValueTuples)
	}
	
	private def createCompactEntity(name: String, values: List[(Int, Any)]) = {
		val indexBitmap = values.map(_._1).foldLeft(0){(acc, index) => acc | (1 << index)} 
		val sortedValues = values.toArray.sortWith{(v1, v2) => v1._1 < v2._1}.map(_._2)
		val svInterned = sortedValues.map(ValuePool.intern(_))
		new CEBitmapImpl(name, ValuePool.intern(indexBitmap), svInterned)
	}
	
	private def get(entityName: String, ce: CEBitmapImpl): Entity = {
	  val indexes = (0 to 31).foldRight(List[Int]()) {(index, indexList) => 
	    if (containsIndex(ce, index)) {
	      index :: indexList
	    } else indexList
	  }
	  Map(indexes.map(CEBitmapMetaData.indexToColumn(entityName, _)).zip(ce.valueArray):_*)
	}
	
	private def containsIndex(ce: CEBitmapImpl, index: Int): Boolean = {
	  (ce.indexBitmap & (1 << index)) > 0
	}
  
  private def validate(entityName: String, entityAsMap: Map[String,Any]): Unit = {
	  if (entityAsMap.keys.size > 32) {
	    throw new Exception("We use an integer as bitmap and cannot store entities with more than 32 values")
	  }
	}
}

class CEBitmapImpl private(val name: String, private val indexBitmap: Int, private val valueArray: Array[Any]) extends CompactEntity{
  
  override def toString() = get.toString
  
  def get : Entity = {
    CEBitmapImpl.get(name, this)
  }
  
  override def hashCode(): Int = {
		val prime = 31;
		var result = 1;
		result = prime * result + indexBitmap;
		result = prime * result + Arrays.hashCode(valueArray.asInstanceOf[Array[Object]]) 
		return result;
	}
	override def equals(obj: Any): Boolean = {
		val anyRef = obj.asInstanceOf[AnyRef]
		if (this eq anyRef)
			return true;
		if (anyRef eq null)
			return false;
		if (!(getClass() eq obj.getClass()))
			return false;
		val other = obj.asInstanceOf[CEBitmapImpl];
		if (indexBitmap != other.indexBitmap)
			return false;
		if (!(Arrays.equals(valueArray.asInstanceOf[Array[Object]], other.valueArray.asInstanceOf[Array[Object]])))
			return false;
		return true;
	}
  
}
