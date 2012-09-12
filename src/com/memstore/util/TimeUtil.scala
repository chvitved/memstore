package com.memstore.util
import java.text.DecimalFormat

object TimeUtil {
  
  val df = new DecimalFormat("#.##")
  
  def printNanos(timeInNanos: Long) : String = {
    df.format(timeInNanos / 1000000.0) + "millis"
  }

}