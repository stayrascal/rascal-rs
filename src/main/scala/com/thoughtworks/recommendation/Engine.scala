package com.thoughtworks.recommendation

import org.apache.predictionio.controller.{Engine, EngineFactory}

case class Query(user: String,
                 num: Int,
                 categories: Option[Set[String]],
                 whiteList: Option[Set[String]],
                 blackList: Option[Set[String]]
                ) extends Serializable

case class PredictedResult(itemScores: Array[ItemScore]) extends Serializable

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
