package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object ListCommands {
  case class LPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LPUSH" +: (key :: value :: values.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class LPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LPUSHX" +: (Seq(key, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class RPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("RPUSH" +: (key :: value :: values.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class RPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("RPUSHX" +: (Seq(key, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class LRange(key: Any, start: Int, stop: Int)(implicit format: Format) extends ListCommand {
    type Ret = List[String]
    val line = multiBulk("LRANGE" +: (Seq(key, start, stop) map format.apply))
    val ret  = (_: RedisReply[_]).asList.flatten // TODO Remove intermediate Option[A]
  }

  case class LLen(key: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LLEN" +: (Seq(format.apply(key))))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class LTrim(key: Any, start: Int, end: Int)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LTRIM" +: (Seq(key, start, end) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class LIndex(key: Any, index: Int)(implicit format: Format) extends ListCommand {
    type Ret = Option[String]
    val line = multiBulk("LINDEX" +: (Seq(key, index) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }

  case class LSet(key: Any, index: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LSET" +: (Seq(key, index, value) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class LRem(key: Any, count: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LREM" +: (Seq(key, count, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class LPop(key: Any)(implicit format: Format) extends ListCommand {
    type Ret = Option[String]
    val line = multiBulk("LPOP" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }

  case class RPop(key: Any)(implicit format: Format) extends ListCommand {
    type Ret = Option[String]
    val line = multiBulk("RPOP" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }

  case class RPopLPush(srcKey: Any, dstKey: Any)(implicit format: Format) extends ListCommand {
    type Ret = Option[String]
    val line = multiBulk("RPOPLPUSH" +: (Seq(srcKey, dstKey) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }

  case class BRPopLPush(srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format) extends ListCommand {
    type Ret = Option[String]
    val line = multiBulk("BRPOPLPUSH" +: (Seq(srcKey, dstKey, timeoutInSeconds) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }

  case class BLPop(timeoutInSeconds: Int, key: Any, keys: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Option[(String, String)]
    val line = multiBulk("BLPOP" +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret  = (_: RedisReply[_]).asListPairs.flatten.headOption
  }

  case class BRPop(timeoutInSeconds: Int, key: Any, keys: Any*)
    (implicit format: Format) extends ListCommand {
    type Ret = Option[(String, String)]
    val line = multiBulk("BRPOP" +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret  = (_: RedisReply[_]).asListPairs.flatten.headOption
  }
}
