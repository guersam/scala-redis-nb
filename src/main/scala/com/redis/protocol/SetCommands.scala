package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object SetCommands {

  trait Op
  case object add extends Op
  case object rem extends Op

  case class SOp(op: Op, key: Any, value: Any, values: Any*)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk((if (op == add) "SADD" else "SREM") +: (key :: value :: values.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class SPop(key: Any)(implicit format: Format) extends SetCommand {
    type Ret = Option[String]
    val line = multiBulk("SPOP" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }
  
  case class SMove(srcKey: Any, destKey: Any, value: Any)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk("SMOVE" +: (Seq(srcKey, destKey, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class SCard(key: Any)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk("SCARD" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class ∈(key: Any, value: Any)(implicit format: Format) extends SetCommand {
    type Ret = Boolean
    val line = multiBulk("SISMEMBER" +: (Seq(key, value) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp
  case object diff extends setOp

  case class ∩∪∖(ux: setOp, key: Any, keys: Any*)(implicit format: Format) extends SetCommand {
    type Ret = Set[String]
    val line = multiBulk(
      (if (ux == inter) "SINTER" else if (ux == union) "SUNION" else "SDIFF") +: (key :: keys.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asSet
  }
  
  case class SUXDStore(ux: setOp, destKey: Any, key: Any, keys: Any*)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk(
      (if (ux == inter) "SINTERSTORE" else if (ux == union) "SUNIONSTORE" else "SDIFFSTORE") +: (destKey :: key :: keys.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class SMembers(key: Any)(implicit format: Format) extends SetCommand {
    type Ret = Set[String]
    val line = multiBulk("SDIFF" +: Seq(key) map format.apply)
    val ret  = (_: RedisReply[_]).asSet
  }

  case class SRandMember(key: Any)(implicit format: Format) extends SetCommand {
    type Ret = Option[String]
    val line = multiBulk("SRANDMEMBER" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk
  }

  case class SRandMembers(key: Any, count: Int)(implicit format: Format) extends SetCommand {
    type Ret = List[String]
    val line = multiBulk("SRANDMEMBER" +: (Seq(key, count) map format.apply))
    val ret  = (_: RedisReply[_]).asList.flatten // TODO remove intermediate Option
  }
}
