package cn.itcast.spark

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

//TODO 利用akka实现spark的启动注册通信
class Master extends Actor {

  //定义个workerMap用于保存注册信息
  private val workerMap = new mutable.HashMap[String, WorkerInfo]()
  //定义个数据集合 转门用于后续的worker排序
  val workerList = new ListBuffer[WorkerInfo]

  //方法在构造函数之后执行 receive方法执行前执行 只执行一次
  override def preStart(): Unit = {
    //master启动之后 进行worker超时检查工作
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, 15000 millis, self, CheckOutTime)
  }

  //不需要用户自己控制
  override def receive: Receive = {
    //用于接收注册信息
    case RegisterMessage(workerID, memory, cores) => {
      //判断是否已经注册 如果没有注册 把信息保存
      if (!workerMap.contains(workerID)) {
        val workerInfo = new WorkerInfo(workerID, memory, cores)
        workerMap.put(workerID, workerInfo)
        //此时还需要一个集合来保存worker信息 便于后续业务需要 根据worker的内存大小排序
        workerList += workerInfo
        //发送注册成功信息给worker
        sender ! RegisterSuccessMessage(workerID + "注册成功")
      }
    }

    //用于匹配心跳信息
    case HeartBeatMessage(workerID) => {
      //判读worker是否已经注册 如果注册 更新其上次的心跳时间
      if (workerMap.contains(workerID)) {
        val workerInfo: WorkerInfo = workerMap(workerID)
        //获取当前时间 就是该worker的心跳时间
        val nowTime: Long = System.currentTimeMillis()
        workerInfo.lastHeartBeatTime = nowTime
      }
    }

    //该匹配主要用于心跳超时检测
    case CheckOutTime => {
      //workerMap.filter(x=>System.currentTimeMillis()-x._2.lastHeartBeatTime>15000)
      //过滤出超时的worker信息
      val outTimeWorker: ListBuffer[WorkerInfo] = workerList.filter(x => System.currentTimeMillis() - x.lastHeartBeatTime > 15000)
      //遍历超时的worker 从workerMap workerList 中剔除
      for (i <- outTimeWorker) {
        workerMap.remove(i.workerID)
        workerList -= i
      }

      //打印当前存活的worker个数
      println("当前活着的worker的个数:"+workerList.size)
      //打印当前存活的worker明细
      println(workerList.sortBy(x=>x.memory).reverse)
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    //定义master的host port
    val host = args(0)
    val port = args(1)

    //配置参数
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
    """.stripMargin

    //配置config 指定akka启动相关参数 configFactory
    val config: Config = ConfigFactory.parseString(configStr)

    //首先创建masterActorSystem
    val masterActorSystem: ActorSystem = ActorSystem.create("masterActorSystem", config)

    //通过masterActorSystem创建Master Actor
    val master: ActorRef = masterActorSystem.actorOf(Props(new Master), "Master")
  }
}