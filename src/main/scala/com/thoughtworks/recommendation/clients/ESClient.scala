package com.thoughtworks.recommendation.clients

import java.util

import com.thoughtworks.recommendation.{ItemScore, PredictedResult}
import grizzled.slf4j.Logger
import org.apache.predictionio.data.storage.Storage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.spark._
import org.joda.time.DateTime

/**
  * Elasticsearch notes:
  * 1) every query clause will affect scores unless it has a constant score and boost: 0
  * 2) The Spark index writer is fast but must assemble all data for the index before the write occurs
  * 3) Many operations must be followed by a refresh before the action takes effect sort of like a transaction commit stemmed, tokenized, etc.
  * Then the values are literal and must match exactly what is in the query (no analyzer)
  */
object ESClient {
  @transient lazy val logger = Logger[this.type]

  private lazy val client = if (Storage.getConfig("ELASTICSEARCH").nonEmpty) {
    new StorageClient(Storage.getConfig("ELASTICSEARCH").get).client
  } else {
    throw new IllegalArgumentException("No Elasticsearch client configuration detected, check your pio-env.sh for property configuration settings")
  }

  def deleteIndex(indexName: String, refresh: Boolean = false): Boolean = {
    if (client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
      val delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
      if (!delete.isAcknowledged) {
        logger.info(s"Index ${indexName} wasn't deleted, but amy hae quietly failed.")
      } else {
        if (refresh) refreshIndex(indexName)
      }
      true
    } else {
      logger.warn(s"Elasticsearch index: ${indexName} wasn't deleted because it didn't exist. This may be an error.")
      false
    }
  }

  def createIndex(indexName: String,
                  indexType: String = "items",
                  fieldNames: List[String],
                  typeMappings: Option[Map[String, String]] = None,
                  refresh: Boolean = false): Boolean = {
    if (!client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists) {
      var mappings =
        """
          |{
          |   "properties": {
        """.stripMargin.replace("\n", "")

      def mappingsField(t: String) = {
        s"""
           |    : {
           |      "type": "${t}",
           |      "index": "not_analyzed",
           |      "norms": {
           |        "enabled": false
           |        }
           |      },
         """.stripMargin.replace("\n", "")
      }

      val mappingsTail =
        """
          |   "id": {
          |     "type": "string",
          |     "index": "not_analyzed",
          |     "norms": {
          |       "enabled": false
          |     }
          |   }
        """.stripMargin.replace("\n", "")

      fieldNames.foreach { fieldName =>
        if (typeMappings.nonEmpty && typeMappings.get.contains(fieldName))
          mappings += (fieldName + mappingsField(typeMappings.get(fieldName)))
        else
          mappings += (fieldName + mappingsField("string"))
      }
      mappings += mappingsTail

      val cir = new CreateIndexRequest(indexName).mapping("items", mappings)
      val create = client.admin().indices().create(cir).actionGet()
      if (!create.isAcknowledged) {
        logger.info(s"Index ${indexName} wasn't created, but may have quietly failed.")
      } else {
        if (refresh) refreshIndex(indexName)
      }
      true
    } else {
      logger.warn(s"Elasticsearch index: ${indexName} wasn't created because it already exists. This may be an error.")
      false
    }
  }

  def hotSwap(alias: String,
              typeName: String = "items",
              indexRDD: RDD[scala.collection.Map[String, Any]],
              fieldNames: List[String],
              typeMappings: Option[Map[String, String]] = None): Unit = {
    val aliasMetadata = client.admin().indices().prepareGetAliases(alias).get().getAliases
    val newIndex = alias + "_" + DateTime.now().getMillis.toString
    createIndex(newIndex, typeName, fieldNames, typeMappings)

    val newIndexURI = s"/${newIndex}/${typeName}"
    indexRDD.saveToEs(newIndexURI, Map("es.mapping.id" -> "id"))

    if (!aliasMetadata.isEmpty && aliasMetadata.get(alias) != null && aliasMetadata.get(alias).get(0) != null) {
      val oldIndex = aliasMetadata.get(alias).get(0).getIndexRouting
      client.admin().indices().prepareAliases()
        .removeAlias(oldIndex, alias)
        .addAlias(newIndex, alias)
        .execute().actionGet()
      deleteIndex(oldIndex)
    } else {
      val indices = util.Arrays.asList(client.admin().indices().prepareGetIndex().get().indices()).get(0)
      if (indices.contains(alias)) {
        deleteIndex(alias)
      }
      client.admin().indices().prepareAliases().addAlias(newIndex, alias).execute().actionGet()
    }

    val indices = util.Arrays.asList(client.admin().indices().prepareGetIndex().get().indices()).get(0)
    indices.map { index =>
      if (index.contains(alias) && index != newIndex) deleteIndex(index)
    }
  }

  def search(query: String, indexName: String): PredictedResult = {
    val sr = client.prepareSearch(indexName).setSource(query).get()

    if (!sr.isTimedOut) {
      val recs = sr.getHits.getHits.map(hit => ItemScore(hit.getId, hit.getScore.toDouble))
      logger.info(s"Results: ${sr.getHits.getHits.size} retrieved of a possible ${sr.getHits.totalHits()}")
      PredictedResult(recs)
    } else {
      logger.info(s"No results for query ${query}")
      PredictedResult(Array.empty[ItemScore])
    }
  }

  def getSource(indexName: String,
                typeName: String,
                doc: String): util.Map[String, AnyRef] = {
    client.prepareGet(indexName, typeName, doc).execute().actionGet().getSource
  }

  def getIndexName(alias: String): Option[String] = {
    val allIndicesMap = client.admin().indices().getAliases(new GetAliasesRequest(alias)).actionGet().getAliases

    if (allIndicesMap.size() == 1) {
      var indexName: String = ""
      var itr = allIndicesMap.keysIt()
      while (itr.hasNext)
        indexName = itr.next()
      Some(indexName)
    } else {
      logger.warn("There is no 1-1 mapping fo index to alias so deleting the old indexes that are referenced by the alias. This may have been caused by a crashed or stopped `pio train` operation so try running it again.")
      val i = allIndicesMap.keys().toArray.asInstanceOf[Array[String]]
      for (indexName <- i) {
        deleteIndex(indexName, true)
      }
      None
    }
  }

  def getRDD(sc: SparkContext, alias: String, typeName: String): Option[RDD[(String, collection.Map[String, AnyRef])]] = {
    val index = getIndexName(alias)
    if (index.nonEmpty) {
      val indexAsRDD = sc.esRDD(alias + "/" + typeName)
      Some(indexAsRDD)
    } else None
  }


  def refreshIndex(indexName: String): Unit = {
    client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet()
  }

}