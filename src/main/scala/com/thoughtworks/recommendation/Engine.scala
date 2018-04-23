package com.thoughtworks.recommendation

import org.apache.predictionio.controller.{Engine, EngineFactory}

case class Query(user: String,
                 num: Int,
                 genres: Option[Set[String]],
                 whiteList: Option[Set[String]],
                 blackList: Option[Set[String]]
                ) extends Serializable

case class PredictedResult(itemScores: Array[ItemScore]) extends Serializable

//case class ActualResult(buyItems: Array[ItemScore])

case class ActualResult(ratings: Array[RatingEvent])


case class ItemScore(item: String,
                title: String,
                releaseDate: String,
                genres: Map[String, String],
                score: Double) extends Serializable

object ECommerceRecommendationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("ecomm" -> classOf[ECommAlgorithm]),
      classOf[Serving])
  }
}
