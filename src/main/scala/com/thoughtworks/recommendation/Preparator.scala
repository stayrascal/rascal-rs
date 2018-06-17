package com.thoughtworks.recommendation

import org.apache.mahout.math.indexeddataset.{BiDictionary, IndexedDataset}
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.predictionio.controller.PPreparator
import org.apache.predictionio.data.storage.PropertyMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator extends PPreparator[TrainingData, PreparedData] {

  /*def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      ratingEvents = trainingData.ratingEvents,
      buyEvents = trainingData.buyEvents
    )
  }*/

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    var userDictionary: Option[BiDictionary] = None

    val indexedDatasets = trainingData.actions.map { case (eventName, eventRDDs) =>
      val ids = IndexedDatasetSpark(eventRDDs.map(event => (event._1, event._2)), userDictionary)(sc)
      userDictionary = Some(ids.rowIDs)
      (eventName, ids)
    }

    // make sure that all matrices have identical row space since this corresponds to all users
    val numUsers = userDictionary.get.size
    val numPrimary = indexedDatasets.head._2.matrix.nrow

    val rowAdjustedIds = indexedDatasets.map { case (eventName, eventIDs) =>
      (eventName, eventIDs.create(eventIDs.matrix, userDictionary.get, eventIDs.columnIDs).newRowCardinality(numUsers))
    }

    new PreparedData(trainingData.userFieldsRDD, trainingData.itemFieldsRDD, rowAdjustedIds)
  }
}

class PreparedData(val userFields: RDD[(String, PropertyMap)],
                   val itemFields: RDD[(String, PropertyMap)],
                   val actions: List[(String, IndexedDataset)]
                  ) extends Serializable


class PreparedData1(val users: RDD[(String, User)],
                    val items: RDD[(String, Item)],
                    val ratingEvents: RDD[RatingEvent],
                    val buyEvents: RDD[BuyEvent]
                   ) extends Serializable
