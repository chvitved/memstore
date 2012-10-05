package com.memstore.pbutils
import com.memstore.serialization.Serialization.PBValue
import com.memstore.serialization.Serialization.Tombstone

object PBUtils {
  
  def toPBValue(value: Any): PBValue = {
    val vb = PBValue.newBuilder()
    value match {
      case s: String => vb.setString(s)
      case i: Int => vb.setInt(i)
      case l: Long => vb.setLong(l)
      case t: com.memstore.entity.TombStone => vb.setTombstone(Tombstone.newBuilder.build)
      case b: Boolean => vb.setBoolean(b)
      case d: Double => vb.setDouble(d)
    }
    vb.build()
  }
  
  def toValue(pbValue: PBValue) : Any = {
    if (pbValue.hasInt) pbValue.getInt()
    else if (pbValue.hasLong) pbValue.getLong()
    else if(pbValue.hasString) pbValue.getString()
    else if(pbValue.hasTombstone) com.memstore.entity.TombStone.tombStone
    else if (pbValue.hasBoolean) pbValue.getBoolean()
    else if(pbValue.hasDouble) pbValue.getDouble()
    else {
      throw new Exception("got pbvalue with an unknown value..." + pbValue)
    }
  }

}