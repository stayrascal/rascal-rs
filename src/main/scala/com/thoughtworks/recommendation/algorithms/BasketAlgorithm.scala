package com.thoughtworks.recommendation.algorithms

import com.thoughtworks.recommendation.{PreparedData1, Query1}
import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{P2LAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


case class BasketAlgorithmParams(basketWindow: Int,
                                 maxRuleLength: Int,
                                 minSupport: Double,
                                 minConfidence: Double,
                                 minLift: Double,
                                 minBasketSize: Int,
                                 maxNumRulesPerCond: Int // max number of rules per condition
                          ) extends Params

class BasketAlgorithm(val ap: BasketAlgorithmParams) extends P2LAlgorithm[PreparedData1, Model, Query1, PreparedData1] {

  @transient lazy val maxCondLength = ap.maxRuleLength - 1
  @transient lazy val logger = Logger[this.type]


  override def train(sc: SparkContext, pd: PreparedData1) = {
    val windowMillis = ap.basketWindow * 1000
    require(ap.maxRuleLength >= 2, s"maxRuleLength must be at least 2. Current: ${ap.maxRuleLength}.")
    require((ap.minSupport >= 0 && ap.minSupport < 1), s"minSupport must be >= 0 and < 1. Current: ${ap.minSupport}.")
    require((ap.minConfidence >= 0 && ap.minConfidence < 1), s"minSupport must be >= 0 and < 1. Current: ${ap.minSupport}.")
    require((ap.minBasketSize >= 2), s"minBasketSize must be >= 2. Current: ${ap.minBasketSize}.")

    val transactions: RDD[Set[String]] = pd.ratingEvents
      .map(b => (b.user, ItemAndTime(b.item, b.timestamp)))
      .groupByKey()
      .flatMap { case (user, iter) =>
        val sortedList = iter.toList.sortBy(_.t)
        val init = ItemSet[String](Set(sortedList.head.item), sortedList.head.t)
        val basketList = sortedList.tail
          .foldLeft(List[ItemSet[String]](init))((list, itemAndTime) =>
            if ((itemAndTime.t - list.head.lastTime) <= windowMillis) {
              (list.head + itemAndTime) :: list.tail
            } else {
              ItemSet(Set(itemAndTime.item), itemAndTime.t) :: list
            }
          )
        logger.debug(s"user ${user}: ${basketList}. ")
        basketList.map(_.items).filter(_.size >= ap.minBasketSize)
      }.cache()

    val totalTransaction = transactions.count()
    val minSupportCount = ap.minBasketSize * totalTransaction

    logger.debug(s"Transactions: ${transactions.collect.toList}")
    logger.info(s"totalTransaction: ${totalTransaction}")

    // generate item sets
    val itemSets: RDD[Set[String]] = transactions.flatMap { tran => (1 to ap.maxRuleLength).flatMap(n => tran.subsets(n)) }
    logger.debug(s"itemSets: ${itemSets.cache().collect().toList}")

    val itemSetCount: RDD[(Set[String], Int)] = itemSets.map(s => (s, 1))
      .reduceByKey((a, b) => a + b)
      .filter(_._2 >= minSupportCount)
      .cache()

    logger.debug(s"itemSetCount: ${itemSetCount.collect().toList}")

    val rules: RDD[(Set[String], RuleScore)] = itemSetCount
      .filter { case (set, count) => set.size >= 2 }
      .flatMap { case (set, count) => set.map(i => (Set(i), (set - i, count))) }
      .join(itemSetCount)
      .map { case (conseq, ((cond, ruleCnt), conseqCnt)) => (cond, (conseq, conseqCnt, ruleCnt)) }
      .join(itemSetCount)
      .map { case (cond, ((conseq, conseqCnt, ruleCnt), condCnt)) =>
        val support = ruleCnt.toDouble / totalTransaction
        val confidence = ruleCnt.toDouble / condCnt
        val lift = (ruleCnt.toDouble / (condCnt * conseqCnt)) * totalTransaction
        val ruleScore = RuleScore(conseq = conseq.head, support = support, confidence = confidence, lift = lift)
        (cond, ruleScore)
      }
      .filter { case (cond, rs) => (rs.confidence >= ap.minConfidence) && (rs.lift >= ap.minLift) }

    val sortedRules = rules.groupByKey
      .mapValues(iter => iter.toVector.sortBy(_.confidence)(Ordering.Double.reverse).take(ap.maxNumRulesPerCond)).collectAsMap().toMap
    new Model(sortedRules)
  }

  /*override def predict(model: Model, query: Query) = {
    val conds = (1 to maxCondLength).flatMap(n => query.items.getOrElse(Set[String]()).subsets(n))

    val rules = conds.map { cond =>
      model.rules.get(cond).map { vec =>
        val itemScores = vec.take(query.num).map { rs =>
          new ItemScore(
            item = rs.conseq,
            support = rs.support,
            confidence = rs.confidence,
            lift = rs.lift
          )
        }.toArray
        Rule(cond = cond, itemScores = itemScores)
      }
    }.flatten.toArray

    new PredictedResult(rules)
  }*/
  override def predict(model: Model, query: Query1) = ???
}

case class ItemAndTime[T](item: T, t: Long)

case class ItemSet[T](items: Set[T], lastTime: Long) {
  def size = items.size

  def isEmpty = items.isEmpty

  def +(elem: ItemAndTime[T]): ItemSet[T] = {
    val newSet = items + elem.item
    val newLastTime = if (elem.t > lastTime) elem.t else lastTime
    new ItemSet(newSet, newLastTime)
  }
}

case class RuleScore(conseq: String, support: Double, confidence: Double, lift: Double)

class Model(val rules: Map[Set[String], Vector[RuleScore]]) extends Serializable {
  override def toString: String = s"rules: ${rules.size} ${rules.take(2)}..."
}
