package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object SortedSetCommands {
  case class ZAdd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk(
      "ZADD" +:
      (List(key, score, member) ::: scoreVals.toList.map(x => List(x._1, x._2)).flatten) map format.apply
    )
  }
  
  case class ZRem(key: Any, member: Any, members: Any*)(implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("ZREM" +: (key :: member :: members.toList) map format.apply)
  }
  
  case class ZIncrby(key: Any, incr: Double, member: Any)(implicit format: Format) extends RedisCommand[Option[Double]] {
    def line = multiBulk("ZINCRBY" +: (Seq(key, incr, member) map format.apply))
  }
  
  case class ZCard(key: Any)(implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("ZCARD" +: (Seq(key) map format.apply))
  }
  
  case class ZScore(key: Any, element: Any)(implicit format: Format) extends RedisCommand[Option[Double]] {
    def line = multiBulk("ZSCORE" +: (Seq(key, element) map format.apply))
  }

  case class ZRange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) 
    extends RedisCommand[List[A]] {

    def line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") +: (Seq(key, start, end) map format.apply))
  }

  case class ZRangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A])
    extends RedisCommand[List[(A, Double)]] {

    def line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") +:
      (Seq(key, start, end, "WITHSCORES") map format.apply)
    )
  }

  case class ZRangeByScore[A](key: Any,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) extends RedisCommand[List[A]] {

    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    def line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE" +: ((Seq(key, minParam, maxParam) ++ limitEntries) map format.apply)
      else "ZREVRANGEBYSCORE" +: ((Seq(key, maxParam, minParam) ++ limitEntries) map format.apply)
    )
  }

  case class ZRangeByScoreWithScore[A](key: Any,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) extends RedisCommand[List[(A, Double)]] {

    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    def line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE" +: ((Seq(key, minParam, maxParam, "WITHSCORES") ++ limitEntries) map format.apply)
      else "ZREVRANGEBYSCORE" +: ((Seq(key, maxParam, minParam, "WITHSCORES") ++ limitEntries) map format.apply))
  }

  private def zrangebyScoreWithScoreInternal[A](
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)])
          (implicit format: Format, parse: Parse[A]): (List[Any], String, String) = {

    val limitEntries = 
      if(!limit.isEmpty) { 
        "LIMIT" :: limit.toList.flatMap(l => List(l._1, l._2))
      } else { 
        List()
      }

    val minParam = Format.formatDouble(min, minInclusive)
    val maxParam = Format.formatDouble(max, maxInclusive)
    (limitEntries, minParam, maxParam)
  }

  case class ZRank(key: Any, member: Any, reverse: Boolean = false)
                  (implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk((if (reverse) "ZREVRANK" else "ZRANK") +: (Seq(key, member) map format.apply))
  }

  case class ZRemRangeByRank(key: Any, start: Int = 0, end: Int = -1)
                            (implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYRANK" +: (Seq(key, start, end) map format.apply))
  }

  case class ZRemRangeByScore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)
                             (implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYSCORE" +: (Seq(key, start, end) map format.apply))
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp

  case class ZUnionInterStore(ux: setOp, dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)
                             (implicit format: Format)  extends RedisCommand[Long] {
    def line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") +:
      ((Iterator(dstKey, keys.size) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate)).toList) map format.apply
    )
  }

  case class ZUnionInterStoreWeighted(ux: setOp, dstKey: Any, kws: Iterable[Product2[Any,Double]],
                                      aggregate: Aggregate = SUM)(implicit format: Format) extends RedisCommand[Long] {

    def line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") +:
      ((Iterator(dstKey, kws.size) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++ kws.iterator.map(_._2) ++ Iterator("AGGREGATE", aggregate)).toList) map format.apply
    )
  }

  case class ZCount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity,
                    minInclusive: Boolean = true, maxInclusive: Boolean = true)
                   (implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("ZCOUNT" +: (List(key, Format.formatDouble(min, minInclusive), Format.formatDouble(max, maxInclusive)) map format.apply))
  }
}
