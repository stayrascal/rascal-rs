package com.thoughtworks.recommendation

import org.apache.predictionio.controller.{Engine, EngineFactory}

case class Query(user: Option[String] = None,
                 num: Option[Int] = None,
                 items: Option[Set[String]] = None,
                 genres: Option[Set[String]] = None,
                 whiteList: Option[Set[String]] = None,
                 blackList: Option[Set[String]] = None
                ) extends Serializable

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
