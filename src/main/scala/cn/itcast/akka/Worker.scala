package cn.itcast.akka

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

class Worker extends Actor {
  println("worker构造函数执行了...")

  override def preStart(): Unit = {
    println("worker preStart方法也执行了")
    //在worker启动之后 进行注册消息发送 首先需要拿到master的引用 ActorSelect
    val master: ActorSelection = context.actorSelection("akka.tcp://masterActorSystem@192.168.155.47:12345/user/Master")
    master ! "connect"
  }

  override def receive: Receive = {
    case "connect" => {
      println("worker方法执行了...")
    }

    case "success" =>{
      println("注册成功")
    }
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    //定义worker的host port
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
    val workerActorSystem: ActorSystem = ActorSystem.create("masterActorSystem",config)

    //通过masterActorSystem创建Master Actor
    val worker: ActorRef = workerActorSystem.actorOf(Props(new Worker),"Worker")

    //测试给自己发给connect消息
    //worker ! "connect"

  }
}
