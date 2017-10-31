package rcsmatrix.bigdata.demo.server.comp

import rcsmatrix.bigdata.demo.common.{Constants, UtilTool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}


object LogHourComp {
  def main(args: Array[String]) {
    if (args.length!=1){
      System.err.println("Usage: LogHourComp <data>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("LogHourComp")
    val sc = new SparkContext(conf)
    val sb = new StringBuffer()
    val outPutDir = UtilTool.getOutPutDir(Constants.DM_PATH,args(0),"log_comp")
    UtilTool.deleteExistsFile(new Configuration(),new Path(outPutDir))
    val rdd = sc.textFile(UtilTool.getDwIntPutPath(args(0),"boos_agent_comp"))
      .map(line => {val arr = line.split(Constants.VERTICAL, -1)
        (args(0),(arr(4).toLong,0l,0l,0l))
      }) union
      sc.textFile(UtilTool.getDwIntPutPath(args(0),"register_comp"))
        .map(line => {val arr = line.split(Constants.VERTICAL, -1)
          (args(0),(0l,arr(8).toLong,0l,0l))
        }) union
      sc.textFile(UtilTool.getDwIntPutPath(args(0),"user_dev_change_comp"))
        .map(line => {val arr = line.split(Constants.VERTICAL, -1)
        (args(0),(0l,0l,arr(2).toLong,0l))
        }) union
      sc.textFile(UtilTool.getDwIntPutPath(args(0),"tmnl_open_user_info_comp"))
        .map(line => {val arr = line.split(Constants.VERTICAL, -1)
        (args(0),(0l,0l,0l,arr(4).toLong))
        })
    rdd.reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2,a._3 + b._3, a._4 + b._4)
    }).map(
      line => {
        sb.delete(0, sb.length()).append(line._1)
          .append(Constants.VERTI).append(line._2._1)
          .append(Constants.VERTI).append(line._2._2)
          .append(Constants.VERTI).append(line._2._3)
          .append(Constants.VERTI).append(line._2._4)
      })
      .saveAsTextFile(outPutDir)
    sc.stop()
  }
}

