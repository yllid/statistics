package rcsmatrix.bigdata.demo.domain.ods

import rcsmatrix.bigdata.demo.common.Constants

/**
  * boss代理日志实体类
  */
class BoosAgentLog extends Serializable{
  var msisdn = "" //用户手机号
  var operationTime = "" //操作时间
  var operationType = "" //操作类型
  var operationResult = "" //操作结果
  var imei = "" //终端IMEI

  var dataKey = "" //分组输出时间Key,如:2017120201

  override def toString():String = {
    new StringBuffer().append(msisdn).append(Constants.VERTI).append(operationTime)
      .append(Constants.VERTI).append(operationType).append(Constants.VERTI)
      .append(operationResult).append(Constants.VERTI).append(imei).toString
  }
}
