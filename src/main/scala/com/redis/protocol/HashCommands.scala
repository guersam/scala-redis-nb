package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object HashCommands {
  case class HSet(key: Any, field: Any, value: Any, nx: Boolean = false)(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk(
      (if (nx) "HSETNX" else "HSET") +: (List(key, field, value) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }
  
  case class HGet(key: Any, field: Any)(implicit format: Format) extends HashCommand {
    type Ret = Option[String]
    val line = multiBulk("HGET" +: (List(key, field) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }
  
  case class HMSet(key: Any, map: Iterable[Product2[Any,Any]])(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk("HMSET" +: ((key :: flattenPairs(map)) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }
  
  case class HMGet(key: Any, fields: Any*)(implicit format: Format) extends HashCommand {
    type Ret = Map[String, String]
    val line = multiBulk("HMGET" +: ((key :: fields.toList) map format.apply))
    val ret  = (_: RedisReply[_]).asList.zip(fields).collect {
      case (Some(value), field) => (format(field), value)
    }.toMap
  }
  
  case class HIncrby(key: Any, field: Any, value: Int)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HINCRBY" +: (List(key, field, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class HExists(key: Any, field: Any)(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk("HEXISTS" +: (List(key, field) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }
  
  case class HDel(key: Any, field: Any, fields: Any*)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HDEL" +: ((key :: field :: fields.toList) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class HLen(key: Any)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HLEN" +: (List(key) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class HKeys(key: Any)(implicit format: Format) extends HashCommand {
    type Ret = List[String]
    val line = multiBulk("HKEYS" +: (List(key) map format.apply))
    val ret  = (_: RedisReply[_]).asList.flatten // TODO remove intermediate Option
  }
  
  case class HVals(key: Any)(implicit format: Format) extends HashCommand {
    type Ret = List[String]
    val line = multiBulk("HVALS" +: (List(key) map format.apply))
    val ret  = (_: RedisReply[_]).asList.flatten // TODO remove intermediate Option
  }
  
  case class HGetall(key: Any)(implicit format: Format) extends HashCommand {
    type Ret = Map[String, String]
    val line = multiBulk("HGETALL" +: (List(key) map format.apply))
    val ret  = (_: RedisReply[_]).asListPairs.flatten.toMap // TODO remove intermediate Option
  }
}
