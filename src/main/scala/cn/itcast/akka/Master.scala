package cn.itcast.akka

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

class Master extends Actor {
  println("master构造函数执行了")

  //方法在构造函数之后执行 receive方法执行前执行 只执行一次
  override def preStart(): Unit = {
    println("master preStart方法执行了")
  }

  //不需要用户自己控制
  override def receive: Receive = {
    case "connect" => {
      println("a client connected.....")

      //接受注册信息 返回注册成功
      sender ! "success"
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

    //测试给自己发给connect信息
    //master ! "connect"

  }
}