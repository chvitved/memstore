package com.memstore.entity.impl.cepb

import com.memstore.entity.CompactEntity
import com.memstore.Types.Entity

import com.memstore.serialization.Serialization.PBEntity

object CEPB {
  
  private def toEntity(pbe: PBEntity) : Entity = {
    null
  }

}

class CEPB private(pbe: PBEntity) extends CompactEntity{
  
  def get() : Entity = null
}