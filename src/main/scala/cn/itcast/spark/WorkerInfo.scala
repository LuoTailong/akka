package cn.itcast.spark

class WorkerInfo(val workerID: String, val memory: Int, val cores: Int) {
  //定义个属性 用于记录上次心跳时间
  var lastHeartBeatTime: Long = _

  override def toString: String = s"workerID:$workerID,$memory,$cores"
}
