package rcsmatrix.bigdata.demo.common

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import rcsmatrix.bigdata.demo.domain.ods.{BoosAgentLog, RegisterLog, TmnlOpenUserInfoLog, UserDevChangeLog}

/**
  * Kryo serialization
  */
class Registrator extends KryoRegistrator(){
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[BoosAgentLog])
    kryo.register(classOf[RegisterLog])
    kryo.register(classOf[TmnlOpenUserInfoLog])
    kryo.register(classOf[UserDevChangeLog])
  }
}


