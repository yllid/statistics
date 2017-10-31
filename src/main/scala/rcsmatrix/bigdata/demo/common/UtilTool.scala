package rcsmatrix.bigdata.demo.common

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object UtilTool {
  val sdf = new SimpleDateFormat("yyyyMMddHH")//2017 1018 10
  /**
    * 时间戳转字符串
    * @param s
    * @return
    */
  def timestampToDate(s: String): String ={
    if(s!=null&&s.length==13) sdf.format(new Date(s.toLong)) else ""
  }
  /**
    * 日期转字符串
    * @param date
    * @param format
    * @return
    */
  def format(date: Date,format: String): String ={
    new SimpleDateFormat(format).format(date)
  }
  /**
    *获取ODS路径
    * @param dateStr
    * @param event
    * @return
    */
  def getOdsPath(dateStr: String, event: String): String = {
    if(dateStr.length==8){
      Constants.ODS_PATH + event+ "/" + dateStr+"/*" //hdfs://spark01:9000/sparkjf/ods_data/http/20151230/*
    } else if (dateStr.length==10){
      Constants.ODS_PATH + event+ "/" + dateStr.substring(0,8)+ "/"+dateStr.substring(8,10)+"/*" //hdfs://spark01:9000/sparkjf/ods_data/http/20151230/18/*
    } else {
      ""
    }
  }
  /**
    *获取ODS路径
    * @param dateStr
    * @return
    */
  def getOdsPath(dateStr: String): String = {
    if(dateStr.length==8){
      Constants.ODS_PATH + "*/" + dateStr+ "/*/*/" //hdfs://spark01:9000/sparkjf/ods_data/*/20151230/*
    } else if (dateStr.length==10){
      Constants.ODS_PATH + "*/"+ dateStr.substring(0,8)+ "/"+dateStr.substring(8,10)+ "/*/*/" //hdfs://spark01:9000/sparkjf/ods_data/*/20151230/18/*
    } else {
      ""
    }
  }
  /**
    *获取输出路径
    * @param dateStr
    * @param event
    * @return
    */
  def getOutPutDir(pathType: String,dateStr: String, event: String): String = {
    if(dateStr.length==8){
      pathType + event+ "/" + dateStr+"/" //hdfs://spark01:9000/sparkjf/dw_data/user_visit_tag_comp/20151230/
    } else if (dateStr.length==10){
      pathType + event+ "/" + dateStr.substring(0,8)+ "/"+dateStr.substring(8,10)+"/" //hdfs://spark01:9000/sparkjf/dw_data/user_visit_tag_comp/20151230/18/
    } else {
      ""
    }
  }
  /**
    *获取输入出路径
    * @param dateStr
    * @param event
    * @return
    */
  def getDwIntPutPath(dateStr: String, event: String): String = {
    if(dateStr.length==10){
      Constants.DW_PATH + event + "/" + dateStr.substring(0, 8) + "/" + dateStr.substring(8,10) + "/*"
    } else if (dateStr.length==8){
      Constants.DW_PATH + event + "/" + dateStr + "/*"
    } else {
      ""
    }
  }
  /**
    *获取输入出路径
    * @param dateStr
    * @param event
    * @return
    */
  def getOdsIntPutPath(dateStr: String, event: String): String = {
    if(dateStr.length==10){
      Constants.ODS_PATH + event + "/" + dateStr.substring(0, 8) + "/" + dateStr.substring(8,10) + "/*/*"
    } else if (dateStr.length==8){
      Constants.ODS_PATH + event + "/" + dateStr + "/*/*"
    } else {
      ""
    }
  }
  /**
    * 删除已存在的文件
    * @param conf
    * @param path
    */
  def deleteExistsFile(conf: Configuration,path: Path){
    var fs :FileSystem = null
    try {
      fs = FileSystem.get(conf)
      //fs =path.getFileSystem(conf)//伪分布式
      // 删除已存在的输出文件
      if (fs.exists(path)) {
        if (fs.delete(path, true)) {
          System.out.println("Delete success to:" + path)
        }
      }
      fs.close();
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (fs != null) {
        try {
          fs.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
  }
}
