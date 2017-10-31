package rcsmatrix.bigdata.demo.server.comp

import rcsmatrix.bigdata.demo.common.{Constants, UtilTool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}


object BoosAgentHourLogComp {
  def main(args: Array[String]) {
    if (args.length!=2){
      System.err.println("Usage: BoosAgentHourLogComp <data> <event>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("BoosAgentHourLogComp")
      //.setMaster("spark://hadoop-01:7077")
      //.setJars(List("/home/hadoop/ideaWork/statistics/out/statistics_jar.jar"))
    val sc = new SparkContext(conf)
    val sb = new StringBuffer()
    val outPutDir = UtilTool.getOutPutDir(Constants.DW_PATH,args(0),"boos_agent_comp")
    UtilTool.deleteExistsFile(new Configuration(),new Path(outPutDir))
    sc.textFile(UtilTool.getOdsIntPutPath(args(0),args(1)))
      .map(line => {
        val arr = line.split(Constants.VERTICAL, -1)
        val map = (sb.delete(0, sb.length()).append(args(0))
          .append(Constants.VERTI).append(arr(2)).append(Constants.VERTI)
          .append(arr(3)).append(Constants.VERTI).append(arr(4)).toString, arr(0))
        map
      }).distinct().map(line => {(line._1, 1)}
    ).reduceByKey ((a, b) => {a + b})
      .map(
        line => {
          sb.delete(0, sb.length()).append(line._1).append(Constants.VERTI).append(line._2)
        })
      .saveAsTextFile(outPutDir)
    sc.stop()
  }
}

