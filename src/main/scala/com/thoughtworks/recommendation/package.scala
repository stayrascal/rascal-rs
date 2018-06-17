package com.thoughtworks

import grizzled.slf4j.Logger
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.mahout.sparkbindings.{SparkDistributedContext, _}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * Utility conversions for IndexedDatasetSpark
  */
package object conversions {

  implicit class IndexedDatasetConversions(val indexedDataset: IndexedDatasetSpark) {
    def toStringMapRDD(actionName: String): RDD[(String, Map[String, Seq[String]])] = {
      @transient lazy val logger = Logger[this.type]


      val rowIDDictionary = indexedDataset.rowIDs
      implicit val sc = indexedDataset.matrix.context.asInstanceOf[SparkDistributedContext].sc
      val rowIDDictionary_bcast = sc.broadcast(rowIDDictionary)

      val columnIDDictionary = indexedDataset.columnIDs
      val columnIDDictionary_bcast = sc.broadcast(columnIDDictionary)

      // May want to mapPartition and create bulk updates as a slight optimization creates an RDD of (itemID, Map[correlatorName, list-of-correlator-values]
      indexedDataset.matrix.rdd.map[(String, Map[String, Seq[String]])] { case (rowNum, itemVector) =>
        var itemList = List[(Int, Double)]()
        for (ve <- itemVector.nonZeroes()) {

          itemList = itemList :+ (ve.index, ve.get)
        }
        val vector = itemList.sortBy { elem => -elem._2 }

        val itemID = rowIDDictionary_bcast.value.inverse.getOrElse(rowNum, "INVALID_ITEM_ID")
        try {
          require(itemID != "INVALID_ITEM_ID", s"Bad row number in matrix, skipping item ${rowNum}")
          require(vector.nonEmpty, s"No values so skipping item ${rowNum}")

          // create a list of element ids
          val values = vector.map { item => columnIDDictionary_bcast.value.inverse.getOrElse(item._1, "") }
          (itemID, Map(actionName -> values))
        } catch {
          case cce: IllegalArgumentException => null.asInstanceOf[(String, Map[String, Seq[String]])]
        }
      }.filter(_ != null)
    }
  }

}
