package com.memstore
import com.memstore.index.IndexImpl

object Types {

  type Entity = Map[String, Any]
  type Index = IndexImpl[AnyRef]
}