package cn.itcast.spark

trait SparkMessage extends Serializable {}

//用于worker向master发送注册信息 要跨网络
case class RegisterMessage(workerID: String, memory: Int, cores: Int) extends SparkMessage

//用于master向worker发送注册成功信息 跨网络
case class RegisterSuccessMessage(msg: String) extends SparkMessage

//用于worker给自己发送心跳提醒 不跨网络
case object HeartBeat

//用于worker给master发送心跳信息 跨网络
case class HeartBeatMessage(workerID:String)extends SparkMessage

//用于master给自己发送消息 不跨网络
case object CheckOutTime