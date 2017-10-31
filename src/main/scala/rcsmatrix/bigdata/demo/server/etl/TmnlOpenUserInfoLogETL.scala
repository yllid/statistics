package rcsmatrix.bigdata.demo.server.etl

import java.util.UUID

import rcsmatrix.bigdata.demo.common.{Constants, EtlTool}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TmnlOpenUserInfoLogETL {
  def main(args: Array[String]) {
    //var args = Array("20","email")ktr
    if (args.length!=2){
      System.err.println("Usage: TmnlOpenUserInfoLogETL <seconds> <event>")
      System.exit(1)
    }
    val event = args(1)
    val conf = new SparkConf().setAppName("TmnlOpenUserInfoLogETL")

      //.setMaster("local[8]")
      //.setMaster("spark://hadoop-01:7077")
      //.setJars(List("/home/hadoop/ideaWork/statistics/out/statistics_jar.jar"))

    val ssc = new StreamingContext(conf, Seconds(args(0).toInt))
    // Kafka configurations
    //如果zookeeper没有offset值或offset值超出范围。那么就给个初始的offset。有smallest、largest可选，
    // 分别表示给当前最小的offset、当前最大的offset。默认largest
    val kafkaParams = Map[String, String]("auto.offset.reset" -> "smallest",
      "metadata.broker.list" -> Constants.BROKERS,"group.id" -> Constants.ETL_GROUP,
      "serializer.class" -> "kafka.serializer.StringEncoder")
    val km = new KafkaManager(kafkaParams)
    //createDirectStream 函数是 Spark 1.3.0 开始引入的，其内部实现是调用 Kafka 的低层次 API，
    //Spark 本身维护 Kafka 偏移量等信息，所以可以保证数据零丢失(但是有些情况下可能会导致数据被读取了两次)，
    km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,Set(event+"_topic"))
    .foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val r = rdd.cache()//缓存RDD
        .map(line => {
          val e = EtlTool.getTmnlOpenUserInfoLog(line._2)//抽取
          if(e.dataKey.length>0) (e.dataKey,e.toString()) else (e.dataKey,line._2)
        }).persist()
        //脏数据
        r.filter(_._1.length==0).map(_._2).saveAsTextFile(Constants.ERROR_ETL_PATH + event + "/" + UUID.randomUUID)
        //正常数据
        val re = r.filter(_._1.length>0)
        re.reduceByKey((a,b) => {("")}).map(_._1).collect().map(e => {re.filter(_._1.equals(e)).map(_._2)
          .saveAsTextFile(Constants.ODS_PATH + event+"/"+ e.substring(0,8)+ "/"+e.substring(8,10)+ "/"+ UUID.randomUUID)
        })}
      // 再更新offsets
      km.updateZKOffsets(rdd)
    })

    //启动Streaming
    ssc.start()
    ssc.awaitTermination()
  }
}
