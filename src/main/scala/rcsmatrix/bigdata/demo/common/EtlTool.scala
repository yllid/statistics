package rcsmatrix.bigdata.demo.common

import rcsmatrix.bigdata.demo.domain.ods.{BoosAgentLog, RegisterLog, TmnlOpenUserInfoLog, UserDevChangeLog}

object EtlTool {
  /**
   * 抽取boss代理日志
   * @param value
   * @return
   */
  def getBoosAgentLog(value: String): BoosAgentLog ={
    var obj = new BoosAgentLog()
    if(value!=null){
      val fileds = value.split(Constants.VERTICAL,-1)//切分字段
      if (fileds.size == 5){
        obj.msisdn = fileds(0)
        obj.operationTime = fileds(1)
        obj.operationType = fileds(2)
        obj.operationResult = fileds(3)
        obj.imei = fileds(4)
        if(fileds(1)!=null&&fileds(1).length==12) obj.dataKey = fileds(1)
      }
    }
    obj
  }
  /**
    * 抽取注册日志
    * @param value
    * @return
    */
  def getRegisterLog(value: String): RegisterLog ={
    var obj = new RegisterLog()
    if(value!=null){
      val fileds = value.split(Constants.VERTICAL,-1)//切分字段
      if (fileds.size == 13){
        obj.cdrType = fileds(0)
        obj.cdrCauseDesc = fileds(1)
        obj.cdrAttribute = fileds(2)
        obj.sessionId = fileds(3)
        obj.icid = fileds(4)
        obj.nodeAddress = fileds(5)
        obj.sipUri = fileds(6)
        obj.serviceRequestTime = fileds(7)
        obj.serviceStartTime = fileds(8)
        obj.serviceEndTime = fileds(9)
        obj.cdrCloseCause = fileds(10)
        obj.returnCode = fileds(11)
        obj.tmnlType = fileds(12)
        if(fileds(7)!=null&&fileds(7).length==12) obj.dataKey = fileds(7)
      }
    }
    obj
  }
  /**
    * 抽取终端开通用户信息日志
    * @param value
    * @return
    */
  def getTmnlOpenUserInfoLog(value: String): TmnlOpenUserInfoLog ={
    var obj = new TmnlOpenUserInfoLog()
    if(value!=null){
      val fileds = value.split(Constants.VERTICAL,-1)//切分字段
      if (fileds.size == 5){
        obj.openAccountTime = fileds(0)
        obj.msisdn = fileds(1)
        obj.appVersion = fileds(2)
        obj.tmnlModel = fileds(3)
        obj.openType = fileds(4)
        if(fileds(0)!=null&&fileds(0).length==12) obj.dataKey = fileds(0)
      }
    }
    obj
  }
  /**
    * 抽取用户设备变更日志
    * @param value
    * @return
    */
  def getUserDevChangeLog(value: String): UserDevChangeLog ={
    var obj = new UserDevChangeLog()
    if(value!=null){
      val fileds = value.split(Constants.VERTICAL,-1)//切分字段
      if (fileds.size == 3){
        obj.devChangeTime = fileds(0)
        obj.msisdn = fileds(1)
        obj.imei = fileds(2)
        if(fileds(0)!=null&&fileds(0).length==12) obj.dataKey = fileds(0)
      }
    }
    obj
  }
}
