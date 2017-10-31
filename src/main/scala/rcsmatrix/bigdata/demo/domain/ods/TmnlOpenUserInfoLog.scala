package rcsmatrix.bigdata.demo.domain.ods

import rcsmatrix.bigdata.demo.common.Constants

/**
  * 终端开通用户信息日志实体类
  */
class TmnlOpenUserInfoLog extends Serializable{
  var openAccountTime = "" //开户时间
  var msisdn = "" //用户手机号
  var appVersion = "" //客户端版本号
  var tmnlModel = "" //终端型号
  var openType = "" //开通类型

  var dataKey = "" //分组输出时间Key,如:2017120201

  override def toString():String = {
    new StringBuffer().append(openAccountTime).append(Constants.VERTI).append(msisdn)
      .append(Constants.VERTI).append(appVersion).append(Constants.VERTI)
      .append(tmnlModel).append(Constants.VERTI).append(openType).toString
  }
}
