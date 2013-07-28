package com.redis.protocol

import com.redis.serialization.Parse
import org.slf4j.LoggerFactory // @TODO remove after bugfix

/**
 * Redis will reply to commands with different kinds of replies. It is always possible to detect the kind of reply
 * from the first byte sent by the server:
 * <li> In a Status Reply the first byte of the reply is "+"</li>
 * <li> In an Error Reply the first byte of the reply is "-"</li>
 * <li> In an Integer Reply the first byte of the reply is ":"</li>
 * <li> In a Bulk Reply the first byte of the reply is "$"</li>
 * <li> In a Multi Bulk Reply the first byte of the reply s "*"</li>
 */

sealed trait RedisReply[T] { self =>

  protected def log = LoggerFactory.getLogger(self.getClass) // @TODO remove after bugfix

  def value: T

  def asAny = value.asInstanceOf[Any]

  def asLong: Long =  ???

  def asString: String = ???

  def asBulk: Option[String] = ???

  def asBoolean: Boolean = ???

  def asList: List[Option[String]] = ???

  def asListPairs: List[Option[(String, String)]] = ???

  def asSet: Set[String] = ???
}

case class IntegerReply(value: Long) extends RedisReply[Long] {
  override final def asLong = value
  override final def asBoolean: Boolean = value > 0
}

case class StatusReply(value: String) extends RedisReply[String] {
  final override def asString = value
  final override def asBoolean = true
}

case class BulkReply(value: Option[String]) extends RedisReply[Option[String]] {
  final override def asBoolean: Boolean = value.isDefined
  final override def asBulk = value
  final override def asString = value.get
}

case class ErrorReply(value: RedisError) extends RedisReply[RedisError] {
  final override def asString = value.message
  final override def asBoolean = false
}

case class MultiBulkReply(value: List[BulkReply]) extends RedisReply[List[BulkReply]] {

  final override def asList: List[Option[String]] = {
    value.map(_.value)
  }

  final override def asListPairs: List[Option[(String, String)]] =
    asList.grouped(2).flatMap {
      case List(Some(a), Some(b)) => Iterator.single(Some((a, b)))
      case _ => Iterator.single(None)
    }.toList

  final override def asSet: Set[String] = asList.flatten.toSet

}