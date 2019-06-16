package cn.itcast.spark

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._

//TODO 利用akka实现spark的启动注册通信
class Worker(val memory: Int, val cores: Int) extends Actor {

  val workerID = UUID.randomUUID().toString

  var master: ActorSelection = _

  override def preStart(): Unit = {
    //在worker启动之后 进行注册消息发送 首先需要拿到master的引用 ActorSelect
    master = context.actorSelection("akka.tcp://masterActorSystem@192.168.155.47:12347/user/Master")
    //拿到master引用 通过样例类发送注册信息
    master ! RegisterMessage(workerID, memory, cores)
  }

  //不断接收信息处理的方法
  override def receive: Receive = {
    case RegisterSuccessMessage(msg) => {
      println(msg)

      //注册成功之后 首次立即进行心跳 后续每个一段时间 定时发送心跳
      //TODO schedule(首次时间 间隔时间 发给谁 发什么) 注意参数的类型
      //TODO 对于使用scala定时调度 一般采用周转的形式满足定时 首先给自己发送一个心跳提醒 再发送心跳
      import context.dispatcher
      //需要手动导入隐式转换
      context.system.scheduler.schedule(0 millis, 10000 millis, self, HeartBeat)
    }

    //用于匹配心跳提醒 然后发送心跳信息
    case HeartBeat => {
      //进行真正的心跳汇报
      master ! HeartBeatMessage(workerID)
    }
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    //定义worker的host port
    val host = args(0)
    val port = args(1)

    //定义参数 用于传递进来的memory cores
    val memory = args(2).toInt
    val cores = args(3).toInt

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
    val workerActorSystem: ActorSystem = ActorSystem.create("masterActorSystem", config)

    //通过masterActorSystem创建Master Actor
    val worker: ActorRef = workerActorSystem.actorOf(Props(new Worker(memory, cores)), "Worker")
  }
}
