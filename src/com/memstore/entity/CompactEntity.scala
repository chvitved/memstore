package com.memstore.entity
import java.util.Arrays
import com.memstore.ValuePool
import com.memstore.Types.Entity

object CompactEntity {
	
	def apply(entityName: String, entity: Entity): CompactEntity = {
	  validate(entityName, entity)
	  val indexValueTuple = EntityMetaData.getAndUpdate(entityName, entity)
      createCompactEntity(entityName, indexValueTuple)
	}
	
	private def createCompactEntity(name: String, values: List[(Int, Any)]) = {
		val indexBitmap = values.map(_._1).foldLeft(0){(acc, index) => acc | (1 << index)} 
		val sortedValues = values.toArray.sortWith{(v1, v2) => v1._1 < v2._1}.map(_._2)
		val svInterned = sortedValues.map(ValuePool.intern(_))
		new CompactEntity(name, ValuePool.intern(indexBitmap), svInterned)
	}
	
	private def get(entityName: String, ce: CompactEntity): Map[String, Any] = {
	  val indexes = (0 to 31).foldRight(List[Int]()) {(index, indexList) => 
	    if (containsIndex(ce, index)) {
	      index :: indexList
	    } else indexList
	  }
	  Map(indexes.map(EntityMetaData.indexToColumn(entityName, _)).zip(ce.valueArray):_*)
	}
	
	private def containsIndex(ce: CompactEntity, index: Int): Boolean = {
	  (ce.indexBitmap & (1 << index)) > 0
	}
  
  private def validate(entityName: String, entityAsMap: Map[String,Any]): Unit = {
	  if (entityAsMap.keys.size > 32) {
	    throw new Exception("We use an integer as bitmap and cannot store entities with more than 32 values")
	  }
	}
}

class CompactEntity private(val name: String, private val indexBitmap: Int, private val valueArray: Array[Any]) {
  
  override def toString() = get.toString
  
  def get : Map[String, Any] = {
    CompactEntity.get(name, this)
  }
  
  //TODO make efficient
  def getValue(attribute: String) = get(attribute)
  
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
		val other = obj.asInstanceOf[CompactEntity];
		if (indexBitmap != other.indexBitmap)
			return false;
		if (!(Arrays.equals(valueArray.asInstanceOf[Array[Object]], other.valueArray.asInstanceOf[Array[Object]])))
			return false;
		return true;
	}
  
}
