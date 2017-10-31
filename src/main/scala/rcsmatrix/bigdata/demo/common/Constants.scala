package rcsmatrix.bigdata.demo.common

/**
 * 系统常量
 * Created by LYL on 2017/10/18 0007.
 */
object Constants {
  final val BROKERS = "hadoop-01:9092,hadoop-02:9092,hadoop-03:9092" //kafka brokers
  final val ETL_GROUP = "etl_group"  //kafka ETL组
  final val STAT_GROUP = "stat_group"  //kafka 统计分析组
  final val JOIN_GROUP = "join_group"  //kafka 统计分析组
  final val HDFS_PATH = "hdfs://hadoop-01:9000/statistics/"  //项目HFDS路径
  final val ERROR_ETL_PATH = HDFS_PATH+"error_data/" +ETL_GROUP+"/" //error流量清单源数据存放的HFDS路径
  final val ERROR_STAT_PATH = HDFS_PATH+"error_data/" +STAT_GROUP+"/"  //error统计源数据存放的HFDS路径
  final val ODS_PATH = HDFS_PATH+"ods_data/"  //ods源数据存放的HFDS路径
  final val DW_PATH = HDFS_PATH+"dw_data/"  //dw数据存放的HFDS路径
  final val DM_PATH = HDFS_PATH+"dm_data/"  //dm数据存放的HFDS路径
  final val VERTI = "|"
  final val VERTICAL = "\\|"
  final val COMMA = ","
}
