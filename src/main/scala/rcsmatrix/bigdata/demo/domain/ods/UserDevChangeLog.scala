package rcsmatrix.bigdata.demo.domain.ods

import rcsmatrix.bigdata.demo.common.Constants

/**
  * 用户设备变更日志实体类
  */
class UserDevChangeLog extends Serializable{
  var devChangeTime ="" //设备变更时间
  var msisdn = "" //用户手机号
  var imei = "" //终端IMEI

  var dataKey = "" //分组输出时间Key,如:2017120201

  override def toString():String = {
    new StringBuffer().append(devChangeTime).append(Constants.VERTI).append(msisdn)
      .append(Constants.VERTI).append(imei).toString
  }
}
