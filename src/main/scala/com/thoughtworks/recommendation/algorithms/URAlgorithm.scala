package com.thoughtworks.recommendation.algorithms

import java.util

import com.thoughtworks.recommendation._
import com.thoughtworks.recommendation.clients.ESClient
import com.thoughtworks.recommendation.models.{PopModel, URModel}
import grizzled.slf4j.Logger
import org.apache.mahout.math.cf.SimilarityAnalysis
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.predictionio.controller.{P2LAlgorithm, Params}
import org.apache.predictionio.data.storage.{Event, PropertyMap}
import org.apache.predictionio.data.store.LEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s
import org.json4s.JsonAST.{JDouble, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.TimeoutException
import scala.concurrent.duration.Duration


/**
  * DefaultMaxQueryEvents: default number of user history events to user this recs query
  * DefaultExpireDateName(expireDate): default name for the expire date property of an item
  * DefaultAvailableDateName(availableDate): default name for and item's available after date
  * DefaultDateName(date): when using a date range in the query this is hte name of the item's date
  * DefaultRecsModel(all): use CF + backfill
  */
object defaultURAlgorithmParams {
  val DefaultMaxEventsPerEventType = 500
  val DefaultNum = 20
  val DefaultMaxCorrelatorsPerEventType = 50
  val DefaultMaxQueryEvents = 100

  val DefaultExpireDateName = "expireDate"
  val DefaultAvailableDateName = "availableDate"
  val DefaultDateName = "date"
  val DefaultRecsModel = "all"
  val DefaultBackfillParams = BackfillField()
  val DefaultBackfillFieldName = "popRank"
}

/**
  *
  * @param appName                    appName
  * @param indexName                  Elasticsearch index name, optional
  * @param typeName                   Elasticsearch type name, optional
  * @param recsModel                  all, collabFiltering, backfill
  * @param eventNames                 Name used to ID all user actions
  * @param blacklistEvents            None means use the primary event, empty array means no filter number of events in user-based recs query
  * @param maxQueryEvents             maxQueryEvents
  * @param maxEventsPerEventType      maxEventsPerEventType
  * @param maxCorrelatorsPerEventType maxCorrelatorsPerEventType
  * @param num                        Default max of recs requested
  * @param userBias                   Will cause the default search engine boost of 1.0
  * @param itemBias                   Will cause the default search engine boost of 1.0
  * @param returnSelf                 query building logic defaults this to false
  * @param fields                     defaults to no fields
  * @param backfillField              leave out for default to popular
  * @param availableDateName          name of date property field for then the item is available
  * @param expireDateName             used as the subject of a dateRange in queries, specifies the name of the item property
  * @param dateName                   dateName
  * @param seed                       seed
  */
case class URAlgorithmParams(appName: String,
                             indexName: String,
                             typeName: String,
                             recsModel: Option[String] = Some(defaultURAlgorithmParams.DefaultRecsModel),
                             eventNames: List[String],
                             blacklistEvents: Option[List[String]] = None,
                             maxQueryEvents: Option[Int] = Some(defaultURAlgorithmParams.DefaultMaxQueryEvents),
                             maxEventsPerEventType: Option[Int] = Some(defaultURAlgorithmParams.DefaultMaxEventsPerEventType),
                             maxCorrelatorsPerEventType: Option[Int] = Some(defaultURAlgorithmParams.DefaultMaxCorrelatorsPerEventType),
                             num: Option[Int] = Some(defaultURAlgorithmParams.DefaultNum),
                             userBias: Option[Int] = None,
                             itemBias: Option[Int] = None,
                             returnSelf: Option[Boolean] = None,
                             fields: Option[List[Field]] = None,
                             backfillField: Option[BackfillField] = None,
                             availableDateName: Option[String] = Some(defaultURAlgorithmParams.DefaultAvailableDateName),
                             expireDateName: Option[String] = Some(defaultURAlgorithmParams.DefaultExpireDateName),
                             dateName: Option[String] = Some(defaultURAlgorithmParams.DefaultDateName),
                             seed: Option[Long] = None
                            ) extends Params

/**
  *
  * @param name         name
  * @param backfillType may be 'hot', or 'trending' also
  * @param eventNames   None means use the algorithm eventNames list, otherwise a list of events
  * @param endDate      used only for tests, specifies the start (oldest date) of the popModel's duration
  * @param duration     number of seconds worth of events to use in calculation of backfill
  */
case class BackfillField(name: String = "popRank",
                         backfillType: String = "popular",
                         eventNames: Option[List[String]] = None,
                         endDate: Option[String] = None,
                         duration: Int = 259200)


class URAlgorithm(val ap: URAlgorithmParams) extends P2LAlgorithm[PreparedData, URModel, Query, PredictedResult] {

  case class BoostableCorrelators(actionName: String, itemIDs: Seq[String], boost: Option[Float])

  case class FilterCorrelators(actionName: String, itemIDs: Seq[String])

  @transient lazy val logger: Logger = Logger[this.type]

  override def train(sc: SparkContext, data: PreparedData): URModel = {
    val dateNames = Some(List(ap.dateName.getOrElse(""), ap.availableDateName.getOrElse(""), ap.expireDateName.getOrElse("")))
    val backfillFieldName = ap.backfillField.getOrElse(BackfillField()).name

    ap.recsModel.getOrElse(defaultURAlgorithmParams.DefaultRecsModel) match {
      case "all" => calcAll(sc, data, dateNames, backfillFieldName)
      case "collabFiltering" => calcAll(sc, data, dateNames, backfillFieldName, popular = false)
      case "backfill" => calcPop(sc, data, dateNames, backfillFieldName)
      case _ => throw new IllegalArgumentException("Bad recsModel in engine definition params, possibly a bad json value.")
    }
  }

  override def predict(model: URModel, query: Query) = {
    logger.info(s"Query received, user id: ${query.user}, item id: ${query.item}")

    val queryEventNames = query.eventNames.getOrElse(ap.eventNames)
    val backfillFieldName = ap.backfillField.getOrElse(BackfillField()).name
    val queryAndBlacklist = buildQuery(ap, query, backfillFieldName)
    val recs: PredictedResult = ESClient.search(queryAndBlacklist._1, ap.indexName)
    recs
  }


  def calcAll(sc: SparkContext,
              data: PreparedData,
              dateNames: Option[List[String]] = None,
              backfillFieldName: String,
              popular: Boolean = true): URModel = {
    require(data.actions.take(1).nonEmpty, s"Primary action in PreparedData cannot be empty. " +
      s"Please check if DataSource generates TrainingData and Preprator generates PreparedData correctly.")

    val backfillParams = ap.backfillField.getOrElse(defaultURAlgorithmParams.DefaultBackfillParams)
    val nonDefaultMappings = Map(backfillParams.name -> "float")


    logger.info("Actions read now creating correlators")


    val cooccurrenceIDSs = SimilarityAnalysis.cooccurrencesIDSs(
      data.actions.map(_._2).toArray,
      randomSeed = ap.seed.getOrElse(System.currentTimeMillis()).toInt,
      maxInterestingItemsPerThing = ap.maxCorrelatorsPerEventType.getOrElse(defaultURAlgorithmParams.DefaultMaxCorrelatorsPerEventType),
      maxNumInteractions = ap.maxEventsPerEventType.getOrElse(defaultURAlgorithmParams.DefaultMaxEventsPerEventType)
    ).map(_.asInstanceOf[IndexedDatasetSpark])
    val cooccurrenceCorrelators = cooccurrenceIDSs.zip(data.actions.map(_._1)).map(_.swap)


    val popModel: Option[RDD[(String, Float)]] = if (popular) {
      val duration = ap.backfillField.getOrElse(defaultURAlgorithmParams.DefaultBackfillParams).duration
      val backfillEvents = backfillParams.eventNames.getOrElse(List(ap.eventNames.head))
      val start = ap.backfillField.getOrElse(defaultURAlgorithmParams.DefaultBackfillParams).endDate
      PopModel.calc(Some(backfillParams.backfillType), backfillEvents, ap.appName, duration, start)(sc)
    } else None
    val allItemPropertiesRDD = if (popModel.nonEmpty) {
      data.itemFields.cogroup[Float](popModel.get).map { case (item, pms) =>
        // Set popular score as a field of Item
        val pm = if (pms._1.nonEmpty && pms._2.nonEmpty) {
          val newPM = pms._1.head.fields + (backfillFieldName -> JDouble(pms._2.head))
          PropertyMap(newPM, pms._1.head.firstUpdated, DateTime.now())
        } else if (pms._2.nonEmpty) {
          PropertyMap(Map(backfillFieldName -> JDouble(pms._2.head)), DateTime.now(), DateTime.now())
        } else {
          PropertyMap(Map.empty[String, JValue], DateTime.now(), DateTime.now())
        }
        (item, pm)
      }
    } else data.itemFields


    logger.info("Correlators created now putting into URModel")
    new URModel(
      Some(cooccurrenceCorrelators),
      Some(allItemPropertiesRDD),
      ap.indexName,
      dateNames,
      typeMappings = Some(nonDefaultMappings)
    )
  }


  def calcPop(sc: SparkContext,
              data: PreparedData,
              dateNames: Option[List[String]] = None,
              backfillFieldName: String = ""): URModel = {
    val backfillParams = ap.backfillField.getOrElse(defaultURAlgorithmParams.DefaultBackfillParams)
    val backfillEvents = backfillParams.eventNames.getOrElse(List(ap.eventNames.head))
    val start = ap.backfillField.getOrElse(defaultURAlgorithmParams.DefaultBackfillParams).endDate
    val popModel: Option[RDD[(String, Float)]] =
      PopModel.calc(Some(backfillParams.backfillType), backfillEvents, ap.appName, backfillParams.duration, start)(sc)
    val popRDD = if (popModel.nonEmpty) {
      // Set popular score as a field of Item
      val model = popModel.get.map { case (item, rank) =>
        val newPM = Map(backfillFieldName -> JDouble(rank))
        (item, PropertyMap(newPM, DateTime.now, DateTime.now))
      }
      Some(model)
    } else None

    val propertiesRDD = if (popModel.nonEmpty) {
      val currentMetadata = ESClient.getRDD(sc, ap.appName, ap.typeName)
      if (currentMetadata.nonEmpty) {
        // Merge Item Popular Rank to Item properties
        Some(popModel.get.cogroup[collection.Map[String, AnyRef]](currentMetadata.get).map {
          case (item, (ranks, pms)) =>
            if (ranks.nonEmpty) pms.head + (backfillFieldName -> ranks.head)
            else if (pms.nonEmpty) pms.head
            else Map.empty[String, AnyRef]
        })
      } else None
    } else None

    new URModel(None, None, ap.indexName, None, propertiesRDD = propertiesRDD, typeMappings = Some(Map(backfillFieldName -> "float")))
  }

  /**
    * Get recent events of the user on items to create the recommendations query from
    *
    * @param query query
    */
  def getBiasedRecentUserActions(query: Query): (Seq[BoostableCorrelators], List[Event]) = {
    val recentEvents: List[Event] = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "user",
        entityId = query.user.get,
        eventNames = Some(query.eventNames.getOrElse(ap.eventNames)),
        targetEntityType = None,
        latest = true,
        timeout = Duration(200, "millis")
      ).toList
    } catch {
      case e: TimeoutException =>
        logger.error(s"Timeout when read recent events. Empty list is used. $e")
        List.empty[Event]
      case e: NoSuchElementException =>
        logger.error("No user id for recs, returning similar items for the item specified")
        List.empty[Event]
      case e: Exception =>
        logger.error(s"Error when read recent events: $e")
        throw e
    }

    val userEventBias: Float = query.userBias.getOrElse(ap.userBias.getOrElse(1f))
    val userEventsBoost = if (userEventBias > 0 && userEventBias != 1) Some(userEventBias) else None
    val rActions = query.eventNames.getOrElse(ap.eventNames).map { action =>
      var items = List[String]()

      for (event <- recentEvents) {
        if (event.event == action && items.size < ap.maxQueryEvents.getOrElse(defaultURAlgorithmParams.DefaultMaxQueryEvents)) {
          items = event.targetEntityId.get :: items
        }
      }
      BoostableCorrelators(action, items.distinct, userEventsBoost)
    }
    (rActions, recentEvents)
  }

  def getBiasedSimilarItems(query: Query): Seq[BoostableCorrelators] = {
    if (query.item.nonEmpty) {
      val m: util.Map[String, AnyRef] = ESClient.getSource(ap.indexName, ap.typeName, query.item.get)
      if (m != null) {
        val itemEventBias = query.itemBias.getOrElse(ap.itemBias.getOrElse(1f))
        val itemEventsBoost = if (itemEventBias > 0 && itemEventBias != 1) Some(itemEventBias) else None

        ap.eventNames.map { action =>
          val items: List[String] = try {
            if (m.containsKey(action) && m.get(action) != null) m.get(action).asInstanceOf[List[String]] else List.empty[String]
          } catch {
            case cce: ClassCastException =>
              logger.warn(s"Bad value in item ${query.item} corresponding to key: $action that was not a List[String] ignored.")
              List.empty[String]
          }
          val rItems = if (items.size <= ap.maxQueryEvents.getOrElse(defaultURAlgorithmParams.DefaultMaxQueryEvents)) items
          else items.slice(0, ap.maxQueryEvents.getOrElse(defaultURAlgorithmParams.DefaultMaxQueryEvents) - 1)

          BoostableCorrelators(action, rItems, itemEventsBoost)
        }
      } else List.empty[BoostableCorrelators]
    } else List.empty[BoostableCorrelators]
  }

  def getBoostedMetadata(query: Query): List[BoostableCorrelators] = {
    val paramsBoostedFields = ap.fields.getOrElse(List.empty[Field])
      .filter(_.bias < 0)
      .map(field => BoostableCorrelators(field.name, field.values, Some(field.bias)))

    val queryBoostedFields = query.fields.getOrElse(List.empty[Field])
      .filter(_.bias >= 0f)
      .map(field => BoostableCorrelators(field.name, field.values, Some(field.bias)))
    (queryBoostedFields ++ paramsBoostedFields).distinct
  }

  def getFilteringMetadata(query: Query): List[FilterCorrelators] = {
    val paramsFilterFields = ap.fields.getOrElse(List.empty[Field])
      .filter(_.bias >= 0)
      .map(field => FilterCorrelators(field.name, field.values))

    val queryFilterFields = query.fields.getOrElse(List.empty[Field])
      .filter(_.bias < 0f)
      .map(field => FilterCorrelators(field.name, field.values))
    (queryFilterFields ++ paramsFilterFields).distinct
  }

  def getFilteringDateRange(query: Query): List[JValue] = {
    var json: List[JValue] = List.empty[JValue]
    val currentDate = query.currentDate.getOrElse(DateTime.now().toDateTimeISO.toString)
    if (query.dateRange.nonEmpty && (query.dateRange.get.after.nonEmpty || query.dateRange.get.before.nonEmpty)) {
      val name = query.dateRange.get.name
      val before = query.dateRange.get.before.getOrElse("")
      val after = query.dateRange.get.after.getOrElse("")
      val rangeStart =
        s"""
           |{
           |  "constant_score": {
           |    "filter": {
           |      "range": {
           |        "$name": {
         """.stripMargin
      val rangeAfter =
        s"""
           |          "gt": "$after"
         """.stripMargin
      val rangeBefore =
        s"""
           |          "lt": "$before"
         """.stripMargin
      val rangeEnd =
        s"""
           |        }
           |      }
           |    },
           |    "boost": 0
           |  }
           |}
         """.stripMargin
      var range = rangeStart
      if (!after.isEmpty) {
        range += rangeAfter
        if (!before.isEmpty) range += ","
      }
      if (!before.isEmpty) range += rangeBefore
      range += rangeEnd
      json = json :+ parse(range)
    } else if (ap.availableDateName.nonEmpty && ap.expireDateName.nonEmpty) {
      val availableDate = ap.availableDateName.get
      val expireDate = ap.expireDateName.get
      val available =
        s"""
           |{
           |  "constant_score": {
           |    "filter": {
           |      "range": {
           |        "$availableDate": {
           |          "lte": "$currentDate"
           |        }
           |      }
           |    },
           |    "boost": 0
           |  }
           |}
        """.stripMargin

      json = json :+ parse(available)
      val expire =
        s"""
           |{
           |  "constant_score": {
           |    "filter": {
           |      "range": {
           |        "$expireDate": {
           |          "gt": "$currentDate"
           |        }
           |      }
           |    },
           |    "boost": 0
           |  }
           |}
        """.stripMargin
      json = json :+ parse(expire)
    } else {
      logger.info("Misconfigured date information, either your engine.json date settings or your query's dateRange is incorrect.\n" +
        " Ingoring date information for this query.")
    }
    json
  }

  /**
    * Create a list of item ids that the user has interacted with or are not be included in recommendations
    *
    * @param userEvents userEvents
    * @param query      query
    * @return
    */
  def getExcludedItems(userEvents: List[Event], query: Query): List[String] = {
    val blacklistedItems = userEvents.filter { event =>
      if (ap.blacklistEvents.nonEmpty) {
        if (ap.blacklistEvents.get == List.empty[String]) false
        else ap.blacklistEvents.get.contains(event.event)
      } else ap.eventNames.head.equals(event.event)
    }.map(_.targetEntityId.getOrElse("")) ++ query.blacklistItems.getOrElse(List.empty[String]).distinct

    val includeSelf = query.returnSelf.getOrElse(ap.returnSelf.getOrElse(false))
    val allExcludedItems = if (!includeSelf && query.item.nonEmpty) blacklistedItems :+ query.item.get
    else blacklistedItems
    allExcludedItems.distinct
  }

  def buildQuery(ap: URAlgorithmParams, query: Query, backfillFieldName: String = "") = {
    try {
      val userRecentEvents: (Seq[BoostableCorrelators], List[Event]) = getBiasedRecentUserActions(query)

      val recentUserHistory: Seq[BoostableCorrelators] = if (ap.userBias.getOrElse(1f) >= 0f)
        userRecentEvents._1.slice(0, ap.maxQueryEvents.getOrElse(defaultURAlgorithmParams.DefaultMaxQueryEvents) - 1)
      else List.empty[BoostableCorrelators]

      val similarItems: Seq[BoostableCorrelators] = if (ap.itemBias.getOrElse(1f) >= 0f)
        getBiasedSimilarItems(query)
      else List.empty[BoostableCorrelators]

      val boostedMetadata = getBoostedMetadata(query)

      val allBoostedCorrelators: Seq[BoostableCorrelators] = recentUserHistory ++ similarItems ++ boostedMetadata

      val recentUserHistoryFilter: List[FilterCorrelators] = if (ap.userBias.getOrElse(1f) < 0f) {
        userRecentEvents._1
          .map(i => FilterCorrelators(i.actionName, i.itemIDs))
          .slice(0, ap.maxQueryEvents.getOrElse(defaultURAlgorithmParams.DefaultMaxQueryEvents) - 1)
      } else List.empty[FilterCorrelators]

      val similarItemsFilter = if (ap.itemBias.getOrElse(1f) < 0f) {
        // getBiasedSimilarItems(query).map(i => FilterCorrelators(i.actionName, i.itemIDs)).toList
        similarItems.map(i => FilterCorrelators(i.actionName, i.itemIDs)).toList
      } else List.empty[FilterCorrelators]

      val filteringMetadata: List[FilterCorrelators] = getFilteringMetadata(query)
      val filteringDateRange: List[JValue] = getFilteringDateRange(query)
      val allFilteringCorrelators: List[FilterCorrelators] = recentUserHistoryFilter ++ similarItemsFilter ++ filteringMetadata

      val numRecs = query.num.getOrElse(ap.num.getOrElse(defaultURAlgorithmParams.DefaultNum))

      val shouldFields: Option[List[JValue]] = if (allBoostedCorrelators.isEmpty) None else {
        Some(allBoostedCorrelators.map { i => render("terms" -> (i.actionName -> i.itemIDs) ~ ("boost" -> i.boost)) }.toList)
      }
      val popModelSort: List[json4s.JValue] = List(parse(
        """
          |{
          |  "constant_score": {
          |    "filter": {
          |      "match_all": {}
          |    },
          |    "boost": 0
          |  }
          |}
          |""".stripMargin))
      val should: List[JValue] = if (shouldFields.isEmpty) popModelSort else shouldFields.get ::: popModelSort

      val mustFields: List[JValue] = allFilteringCorrelators.map { i =>
        render("terms" -> (i.actionName -> i.itemIDs) ~ ("boost" -> 0))
      }
      val must: List[JValue] = mustFields ::: filteringDateRange

      val mustNotFields: JValue = render("id" -> ("values" -> getExcludedItems(userRecentEvents._2, query)) ~ ("boost" -> 0))
      val mustNot: JValue = mustNotFields

      val popQuery = if (ap.recsModel.getOrElse("all") == "all" || ap.recsModel.getOrElse("all") == "backfill") {
        Some(List(
          parse( """{"_score": {"order": "desc"}}"""),
          parse(
            s"""
               |{
               |    "$backfillFieldName": {
               |      "unmapped_type": "double",
               |      "order": "desc"
               |    }
               |}""".stripMargin)))
      } else None
      val json =
        ("size" -> numRecs) ~
          ("query" ->
            ("bool" ->
              ("should" -> should) ~
                ("must" -> must) ~
                ("must_not" -> mustNot) ~
                ("minimum_should_match" -> 1))
            ) ~
          ("sort" -> popQuery)

      val j = compact(render(json))
      logger.info(s"Query: \n{$j}\n")
      (compact(render(json)), userRecentEvents._2)
    } catch {
      case e: IllegalArgumentException => ("", List.empty[Event])
    }
  }
}
