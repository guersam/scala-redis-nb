package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.ListCommands
import scala.concurrent.ExecutionContext

trait ListOperations { this: RedisOps =>
  import ListCommands._

  // LPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  def lpush(key: Any, value: Any, values: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LPush(key, value, values:_*)).mapTo[Long]

  // LPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def lpushx(key: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LPushX(key, value)).mapTo[Long]

  // RPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  def rpush(key: Any, value: Any, values: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(RPush(key, value, values:_*)).mapTo[Long]

  // RPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def rpushx(key: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(RPushX(key, value)).mapTo[Long]

  // LLEN
  // return the length of the list stored at the specified key.
  // If the key does not exist zero is returned (the same behaviour as for empty lists).
  // If the value stored at key is not a list an error is returned.
  def llen(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LLen(key)).mapTo[Long]

  // LRANGE
  // return the specified elements of the list stored at the specified key.
  // Start and end are zero-based indexes.
  def lrange[A](key: Any, start: Int, end: Int)
               (implicit ec: ExecutionContext, timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(LRange(key, start, end)).mapTo[List[String]] map (_ map parse)

  // LTRIM
  // Trim an existing list so that it will contain only the specified range of elements specified.
  def ltrim(key: Any, start: Int, end: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LTrim(key, start, end)).mapTo[Boolean]

  // LINDEX
  // return the especified element of the list stored at the specified key.
  // Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
  def lindex[A](key: Any, index: Int)(implicit ec: ExecutionContext, timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(LIndex(key, index)).mapTo[Option[String]] map (_ map parse)

  // LSET
  // set the list element at index with the new value. Out of range indexes will generate an error
  def lset(key: Any, index: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LSet(key, index, value)).mapTo[Boolean]

  // LREM
  // Remove the first count occurrences of the value element from the list.
  def lrem(key: Any, count: Int, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(LRem(key, count, value)).mapTo[Long]

  // LPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def lpop[A](key: Any)(implicit ec: ExecutionContext, timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(LPop(key)).mapTo[Option[String]] map (_ map parse)

  // RPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def rpop[A](key: Any)(implicit ec: ExecutionContext, timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(RPop(key)).mapTo[Option[String]] map (_ map parse)

  // RPOPLPUSH
  // Remove the first count occurrences of the value element from the list.
  def rpoplpush[A](srcKey: Any, dstKey: Any)
                  (implicit ec: ExecutionContext, timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(RPopLPush(srcKey, dstKey)).mapTo[Option[String]] map (_ map parse)

  def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)
                   (implicit ec: ExecutionContext, timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(BRPopLPush(srcKey, dstKey, timeoutInSeconds)).mapTo[Option[String]] map (_ map parse)

  def blpop[K <: Any, V](timeoutInSeconds: Int, key: K, keys: K*)
                (implicit ec: ExecutionContext, timeout: Timeout, format: Format, parseK: Parse[K], parseV: Parse[V]) =
    clientRef.ask(BLPop(timeoutInSeconds, key, keys:_*)).mapTo[Option[(String, String)]] map (_ map {
      case (k, v) => (parseK(v), parseV(v))
    })

  def brpop[K <: Any, V](timeoutInSeconds: Int, key: K, keys: K*)
                (implicit ec: ExecutionContext, timeout: Timeout, format: Format, parseK: Parse[K], parseV: Parse[V]) =
    clientRef.ask(BRPop(timeoutInSeconds, key, keys:_*)).mapTo[Option[(String, String)]] map (_ map {
      case (k, v) => (parseK(v), parseV(v))
    })
}
