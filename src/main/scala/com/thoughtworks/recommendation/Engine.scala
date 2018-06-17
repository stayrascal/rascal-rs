package com.thoughtworks.recommendation

import org.apache.predictionio.controller.{Engine, EngineFactory}

/**
  * The Query spec with optional values. The only hard rule is that there must be either a user or an item id.
  * All other values are optional.
  *
  * @param user           must be a user or item id
  * @param userBias       whatever is in algorithm params or 1
  *                       Bias > 0 to < 1 : lower ranking for items that match the condition.
  *                       Bias = 1: no effect.
  *                       Bias > 1: raise the ranking for items that match the condition.
  *                       Bias = 0: exclude all items that match the condition.
  *                       Bias < 0: A negative bias will only return items that meet the condition.
  * @param item           must be a user or item id
  * @param itemBias       whatever is in algorithm params or 1
  * @param fields         whatever is in algorithm params or None
  * @param currentDate    if used will override dateRange filter, currentDate must lie between the item's expireDateName value and availableDateName value, all are ISO 8601 dates
  * @param dateRange      Optional before and after filter applied to a date field
  * @param blacklistItems whatever is in algorithm params or None
  * @param returnSelf     means for an item query should the item itself be returned, defaults to what is in the algorithm params or false
  * @param num            whatever is in algorithm params, which itself has a default probably 20
  * @param eventNames     names used to ID all user actions
  */
case class Query(user: Option[String] = None,
                 userBias: Option[Float] = None,
                 item: Option[String] = None,
                 itemBias: Option[Float] = None,
                 fields: Option[List[Field]] = None,
                 currentDate: Option[String] = None,
                 dateRange: Option[DateRange] = None,
                 blacklistItems: Option[List[String]] = None,
                 returnSelf: Option[Boolean] = None,
                 num: Option[Int] = None,
                 eventNames: Option[List[String]]
                ) extends Serializable

case class Query1(user: Option[String] = None,
                  num: Option[Int] = None,
                  items: Option[Set[String]] = None,
                  genres: Option[Set[String]] = None,
                  whiteList: Option[Set[String]] = None,
                  blackList: Option[Set[String]] = None
                 ) extends Serializable

case class Field(name: String, values: Array[String], bias: Float) extends Serializable

case class DateRange(name: String, before: Option[String], after: Option[String]) extends Serializable

case class PredictedResult(itemScores: Array[ItemScore]) extends Serializable

//case class ActualResult(buyItems: Array[ItemScore])

case class ActualResult(ratings: Array[RatingEvent])


case class ItemScore(item: String, score: Double) extends Serializable

object ECommerceRecommendationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("ecomm" -> classOf[ECommAlgorithm]),
      classOf[Serving])
  }
}
