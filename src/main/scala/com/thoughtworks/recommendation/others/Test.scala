package com.thoughtworks.recommendation.others

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    val data = spark.read.format("libsvm").load("/Users/zpwu/workspace/scala/pio/recommendation/datasample_libsvm_data.txt")

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    predictions.select("prediction", "label", "features").show(5)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println(s"Learned regression forest model:\n ${rfModel.toDebugString}")
  }
}
