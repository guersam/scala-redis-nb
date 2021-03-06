package com.redis.protocol

import RedisCommand._
import com.redis.serialization.Format


object NodeCommands {

  case class Save(bg: Boolean = false) extends RedisCommand[Boolean] {
    def line = multiBulk(Seq((if (bg) "BGSAVE" else "SAVE")))
  }

  case object LastSave extends RedisCommand[Long] {
    def line = multiBulk(Seq("LASTSAVE"))
  }

  case object Shutdown extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("SHUTDOWN"))
  }

  case object BGRewriteAOF extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("BGREWRITEAOF"))
  }

  case class Info(section: String) extends RedisCommand[Option[String]] {
    def line = multiBulk(Seq("INFO", section))
  }

  case object Monitor extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("MONITOR"))
  }

  case class SlaveOf(options: Any)(implicit format: Format) extends RedisCommand[Boolean] {
    def line = multiBulk(
      options match {
        case (h: String, p: Int) => "SLAVEOF" +: (Seq(h, p) map format.apply)
        case _ => Seq("SLAVEOF", "NO", "ONE")
      }
    )
  }

  case object ClientGetName extends RedisCommand[Option[String]] {
    def line = multiBulk(Seq("CLIENT", "GETNAME"))
  }

  case class ClientSetName(name: String) extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("CLIENT", "SETNAME", name))
  }

  case class ClientKill(ipPort: String) extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("CLIENT", "KILL", ipPort))
  }

  case object ClientList extends RedisCommand[Option[String]] {
    def line = multiBulk(Seq("CLIENT", "LIST"))
  }

  case class ConfigGet(globStyleParam: String) extends RedisCommand[Option[String]] {
    def line = multiBulk(Seq("CONFIG", "GET", globStyleParam))
  }

  case class ConfigSet(param: String, value: Any)(implicit format: Format) extends RedisCommand[Boolean] {
    def line = multiBulk("CONFIG" +: (Seq("SET", param, value) map format.apply))
  }

  case object ConfigResetStat extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("CONFIG", "RESETSTAT"))
  }

  case object ConfigRewrite extends RedisCommand[Boolean] {
    def line = multiBulk(Seq("CONFIG", "REWRITE"))
  }
}
