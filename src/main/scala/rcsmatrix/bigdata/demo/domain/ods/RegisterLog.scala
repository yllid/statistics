package rcsmatrix.bigdata.demo.domain.ods

import rcsmatrix.bigdata.demo.common.Constants

/**
  * 注册日志实体类
  */
class RegisterLog extends Serializable{
  var cdrType = "" //话单类型
  var cdrCauseDesc = "" //话单原因说明
  var cdrAttribute = "" //话单属性
  var sessionId = "" //sessionId
  var icid = "" //计费标识
  var nodeAddress =""//注册所在的S-CSCF节点信息
  var sipUri ="" //called-Party-Address（在注册话单中，该字段用于表示注册的用户标识）
  var serviceRequestTime ="" //服务请求时间
  var serviceStartTime ="" //服务响应时间
  var serviceEndTime ="" //服务结束时间
  var cdrCloseCause = "" //话单关闭原因
  var returnCode = "" //业务请求返回码
  var tmnlType = "" //终端类型

  var dataKey = "" //分组输出时间Key,如:2017120201

  override def toString():String = {
    new StringBuffer().append(cdrType).append(Constants.VERTI).append(cdrCauseDesc)
      .append(Constants.VERTI).append(cdrAttribute).append(Constants.VERTI)
      .append(sessionId).append(Constants.VERTI).append(icid)
      .append(Constants.VERTI).append(nodeAddress)
      .append(Constants.VERTI).append(sipUri).append(Constants.VERTI)
      .append(serviceRequestTime).append(Constants.VERTI).append(serviceStartTime)
      .append(Constants.VERTI).append(serviceEndTime)
      .append(Constants.VERTI).append(cdrCloseCause).append(Constants.VERTI)
      .append(returnCode).append(Constants.VERTI).append(tmnlType).toString
  }
}
