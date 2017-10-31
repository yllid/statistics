import org.apache.spark.util.random
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by LYL on 下午4:07.
  */
class SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://Master:7077")
      .setJars(List("/home/hadoop/ideaWork/statistics/out/statisticsjar"))

    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = 12 * 2 - 1
      val y = 13 * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
