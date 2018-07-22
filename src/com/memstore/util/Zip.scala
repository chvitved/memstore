package com.memstore.util
import java.util.zip.GZIPOutputStream
import java.io.ByteArrayOutputStream

object Zip {
  
  def zip(data: Array[Byte]) : Array[Byte] = {
	val baos = new ByteArrayOutputStream()		
    val zos = new GZIPOutputStream(baos);	
	zos.write(data);
	zos.flush();
	zos.close();
	baos.toByteArray()
  }
  
  //	public static byte[] unzip(byte[] data) {
//		try {
//			ByteArrayInputStream bais = new ByteArrayInputStream(data);
//			GZIPInputStream zin = new GZIPInputStream(bais);
//			//InflaterInputStream zin = new InflaterInputStream(bais);
//			ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			IOUtils.copy(zin, baos);
//			zin.close();
//			bais.close();
//			baos.flush();
//			baos.close();
//            return baos.toByteArray();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//	}

}