package com.thoughtworks.recommendation

import org.apache.predictionio.controller._


case class PrecisionAtK(k: Int, ratingThreshold: Double = 2.0)
  extends OptionAverageMetric[EmptyEvaluationInfo, Query1, PredictedResult, ActualResult] {
  require(k > 0, "k must be greater than 0")

  override def header: String = s"Precision@K (k=$k, threshold=$ratingThreshold)"

  override def calculate(q: Query1, p: PredictedResult, a: ActualResult): Option[Double] = {
    val positives: Set[String] = a.ratings.filter(_.rating > ratingThreshold).map(_.item).toSet

    if (positives.isEmpty) {
      None
    } else {
      val topCount: Int = p.itemScores.take(k).filter(is => positives(is.item)).size
      Some(topCount.toDouble / positives.size)
    }
  }
}

case class RecallAtK(k: Int, ratingThreshold: Double = 2.0)
  extends OptionAverageMetric[EmptyEvaluationInfo, Query1, PredictedResult, ActualResult] {
  require(k > 0, "k must be greater than 0")

  override def header: String = s"Recall@K (k=$k, threshold=$ratingThreshold)"

  override def calculate(q: Query1, p: PredictedResult, a: ActualResult): Option[Double] = {
    val positives: Set[String] = a.ratings.filter(_.rating > ratingThreshold).map(_.item).toSet

    if (positives.isEmpty) {
      None
    } else {
      val topCount: Int = p.itemScores.take(k).filter(is => positives(is.item)).size
      Some(topCount.toDouble / k)
    }
  }
}

case class PositiveCount(ratingThreshold: Double = 2.0) extends AverageMetric[EmptyEvaluationInfo, Query1, PredictedResult, ActualResult] {

  override def header = s"PositiveCount (threshold=$ratingThreshold)"

  override def calculate(q: Query1, p: PredictedResult, a: ActualResult): Double = {
    a.ratings.count(_.rating > ratingThreshold)
  }
}

object ComprehensiveRecommendationEvaluation extends Evaluation {
  val ratingThresholds = Seq(2.0, 3.0, 4.0)
  val ks = Seq(50, 200, 300)

  engineEvaluator = (
    ECommerceRecommendationEngine(),
    MetricEvaluator(
      metric = PrecisionAtK(k = 3, ratingThreshold = 2.0),
      otherMetrics = (
        (for (r <- ratingThresholds) yield PositiveCount(ratingThreshold = r)) ++
          (for (r <- ratingThresholds; k <- ks) yield PrecisionAtK(k = k, ratingThreshold = r)) ++
            (for (r <- ratingThresholds; k <- ks) yield RecallAtK(k = k, ratingThreshold = r))
        ))
  )
}

trait BaseEngineParamsList extends EngineParamsGenerator {
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appName = "recommender", evalParams = Some(DataSourceEvalParams(kFold = 5, queryNum = 10)))
  )
}

object EngineParamsList extends BaseEngineParamsList {
  engineParamsList = for (rank <- Seq(10, 20); numIterations <- Seq(10, 15))
    yield baseEP.copy(algorithmParamsList = Seq(("ecomm", ECommAlgorithmParams(
      appName = "recommender",
      unseenOnly = true,
      seenEvents = List("buy", "rating"),
      similarEvents = List("rating"),
      rank = rank,
      numIterations = numIterations,
      lambda = 0.01,
      Some(3)))))
}