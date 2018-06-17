package com.thoughtworks.recommendation.models

import com.thoughtworks.conversions.IndexedDatasetConversions
import com.thoughtworks.recommendation.algorithms.URAlgorithmParams
import com.thoughtworks.recommendation.clients.ESClient
import grizzled.slf4j.Logger
import org.apache.mahout.math.indexeddataset.IndexedDataset
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.predictionio.controller.{PersistentModel, PersistentModelLoader}
import org.apache.predictionio.data.storage.PropertyMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s.JsonAST.JArray
import org.json4s._

class URModel(coocurrenceMatrices: Option[List[(String, IndexedDataset)]],
              fieldsRDD: Option[RDD[(String, PropertyMap)]],
              indexName: String,
              dateNames: Option[List[String]] = None,
              nullModel: Boolean = false,
              typeMappings: Option[Map[String, String]] = None,
              propertiesRDD: Option[RDD[collection.Map[String, Any]]] = None
             ) extends PersistentModel[URAlgorithmParams] {

  @transient lazy val logger = Logger[this.type]

  def save(id: String, params: URAlgorithmParams, sc: SparkContext): Boolean = {
    if (nullModel) throw new IllegalArgumentException("Saving a null model created from loading an old one.")

    val esIndexURI = s"${params.indexName}/${params.typeName}"

    logger.info("Converting cooccurrence matrices into correlators")

    // For ES we need to create the entire index in an rdd of maps, one per item so we'll use convert coocurrence
    // into correlators as RDD[(itemID, (actionName, Seq(itemID)))] do they need to be in Elasticsearch format
    val correlators = if (coocurrenceMatrices.nonEmpty) coocurrenceMatrices.get.map { case (actionName, dataset) =>
      dataset.asInstanceOf[IndexedDatasetSpark].toStringMapRDD(actionName).asInstanceOf[RDD[(String, Map[String, Any])]]
    } else {
      List.empty[RDD[(String, Map[String, Any])]]
    }

    logger.info(s"Getting a list of action name strings")
    val allActions = coocurrenceMatrices.getOrElse(List.empty[(String, IndexedDatasetSpark)]).map(_._1)

    logger.info(s"Ready to pass date fields names to closure ${dateNames}")
    val closureDateNames = dateNames.getOrElse(List.empty[String])

    // convert the PropertyMap into Map[String, Seq[String]] for ES
    logger.info("Converting PropertyMap into Elasticsearch style rdd")
    var properties = List.empty[RDD[(String, Map[String, Any])]]
    var allPropKeys = List.empty[String]
    if (fieldsRDD.nonEmpty) {
      properties = List(fieldsRDD.get.map { case (item, pm) =>
        var m: Map[String, Any] = Map()
        for (key <- pm.keySet) {
          val k = key
          val v = pm.get[JValue](key)
          try {
            pm.get[JValue](key) match {
              case JArray(list) =>
                val l = list.map {
                  case JString(s) => s
                  case _ => ""
                }
                m = m + (key -> l)
              case JString(s) =>
                if (closureDateNames.contains(key)) {
                  val dateTime = new DateTime(s)
                  val date: java.util.Date = dateTime.toDate
                  m = m + (key -> date)
                }
              case JDouble(rank) =>
                m = m + (key -> rank)
              case JInt(someInt) =>
                m = m + (key -> someInt)
            }
          } catch {
            case e: ClassCastException => e
            case e: IllegalArgumentException => e
            case e: MatchError => e
          }
        }
        (item, m)
      })
      allPropKeys = properties.head.flatMap(_._2.keySet).distinct.collect().toList
    }

    // These need to be indexed with "not_analyzed" and no norms so have to collect all field names before ES index create
    val allFields = (allActions ++ allPropKeys).distinct

    if (propertiesRDD.isEmpty) {
      // Elasticsearch takes a Map with all fields, not a tuple
      logger.info("Grouping all correlators into doc + fields for writing to index")
      logger.info(s"Finding non-empty RDDs from a list of ${correlators.length} correlators and ${properties.length} properties")
      val esRDDs: List[RDD[(String, Map[String, Any])]] = (correlators ::: properties)
      if (esRDDs.nonEmpty) {
        val esFields = groupAll(esRDDs).map { case (item, map) =>
          val esMap = map + ("id" -> item)
          esMap
        }

        // create a new index then hot-swap the new index by re-aliasing to it then delete old index
        ESClient.hotSwap(params.indexName, params.typeName, esFields.asInstanceOf[RDD[scala.collection.Map[String, Any]]], allFields, typeMappings)
      } else logger.warn("No data to write, May have been caused by a failed or stopped `pio train`, tyr running it again")
    } else {
      // This happens when updating only the popularity backfill model but to do a hotSwap we need to dup the entire index
      ESClient.hotSwap(params.indexName, params.typeName, propertiesRDD.get, allFields, typeMappings)
    }
    true
  }

  def groupAll(fields: Seq[RDD[(String, (Map[String, Any]))]]): RDD[(String, (Map[String, Any]))] = {
    if (fields.size > 1) {
      fields.head.cogroup[Map[String, Any]](groupAll(fields.drop(1))).map { case (key, pairMapSeqs) =>
        val rdd1Maps = pairMapSeqs._1.foldLeft(Map.empty[String, Any])(_ ++ _)
        val rdd2Maps = pairMapSeqs._2.foldLeft(Map.empty[String, Any])(_ ++ _)
        val fullMap = rdd1Maps ++ rdd2Maps
        (key, fullMap)
      }
    } else fields.head
  }

  override def toString: String = {
    s"URModel in Elasticsearch at index: ${indexName}"
  }
}

object URModel extends PersistentModelLoader[URAlgorithmParams, URModel] {
  @transient lazy val logger = Logger[this.type]

  /**
    * This is actually only used to read saved values and since they are in Elasticsearch we don't need to read this means
    * we create a null model since it will not be used.
    *
    * @param id
    * @param params
    * @param sc
    * @return
    */
  def apply(id: String, params: URAlgorithmParams, sc: Option[SparkContext]): URModel = {
    val urm = new URModel(null, null, null, nullModel = true)
    logger.info("Create dummy null model")
    urm
  }
}
