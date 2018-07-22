package com.datagenerator

object MemUtil {
  def printMem() {
	System.gc();
	val r = Runtime.getRuntime();
	val mem = (r.totalMemory() - r.freeMemory()) / (1024*1024); //megabytes
	println("used mem: " + mem);
  }


}