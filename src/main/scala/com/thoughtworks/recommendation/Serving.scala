package com.thoughtworks.recommendation

import org.apache.predictionio.controller.LServing

class Serving
  extends LServing[Query1, PredictedResult] {

  override
  def serve(query: Query1, predictedResults: Seq[PredictedResult]): PredictedResult = {
    predictedResults.head
  }
}
