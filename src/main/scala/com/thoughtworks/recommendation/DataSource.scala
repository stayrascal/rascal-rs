package com.thoughtworks.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{EmptyEvaluationInfo, PDataSource, Params}
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(appName: String, evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams) extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override def readTraining(sc: SparkContext): TrainingData = {

    val usersRDD: RDD[(String, User)] = getUsersRDD(sc)
    val itemsRDD: RDD[(String, Item)] = getItemsRDD(sc)
    val ratingEventRDD: RDD[RatingEvent] = getRatingEventsRDD(sc)
    val buyEventRDD: RDD[BuyEvent] = getBuyEventsRDD(sc)

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      ratingEvents = ratingEventRDD,
      buyEvents = buyEventRDD
    )
  }

  override def readEval(sc: SparkContext): Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(!dsp.evalParams.isEmpty, "Must specify evalParams")

    val evalParams = dsp.evalParams.get

    val kFold = evalParams.kFold

    val usersRDD: RDD[(String, User)] = getUsersRDD(sc)
    val itemsRDD: RDD[(String, Item)] = getItemsRDD(sc)
    val ratingsRDD: RDD[RatingEvent] = getRatingEventsRDD(sc)
    val buyRDD: RDD[BuyEvent] = getBuyEventsRDD(sc)

    val ratings: RDD[(RatingEvent, Long)] = ratingsRDD.zipWithIndex().cache()

    (0 until kFold).map {
      idx => {
        val trainingData = ratings.filter(_._2 % kFold != idx).map(_._1)
        val testingData = ratings.filter(_._2 % kFold != idx).map(_._1)
        val testUsers: RDD[(String, Iterable[RatingEvent])] = testingData.groupBy(_.user)
        (new TrainingData(
          users = usersRDD,
          items = itemsRDD,
          ratingEvents = trainingData,
          buyEvents = buyRDD
        ), new EmptyEvaluationInfo(),
          testUsers.map { case (user, ratings) => (Query(Some(user), Some(evalParams.queryNum)), ActualResult(ratings.toArray)) }
        )
      }
    }


  }

  private def getRatingEventsRDD(sc: SparkContext): RDD[RatingEvent] = {
    PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("rating")),
      targetEntityType = Some(Some("item")))(sc).map { event =>
      event.properties
      try {
        RatingEvent(
          user = event.entityId,
          item = event.targetEntityId.get,
          rating = event.properties.get[Double]("rating"),
          timestamp = event.eventTime.getMillis
        )
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert $event to RatingEvent. Exception: $e.")
          throw e
        }
      }
    }
  }

  private def getBuyEventsRDD(sc: SparkContext): RDD[BuyEvent] = {
    PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("buy")),
      targetEntityType = Some(Some("item")))(sc).map { event =>
      try {
        BuyEvent(
          user = event.entityId,
          item = event.targetEntityId.get,
          timestamp = event.eventTime.getMillis
        )
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert $event to BuyEvent. Exception: $e.")
          throw e
        }
      }
    }
  }


  private def getItemsRDD(sc: SparkContext) = {
    PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {

        Item(
          id = entityId,
          title = properties.get[String]("title"),
          releaseDate = properties.get[String]("release_date"),
          genres = Constants.GENDER_FIELDS.map(key => properties.getOrElse[String](key, "0"))
        )
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()
  }

  private def getUsersRDD(sc: SparkContext) = {
    PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {

        User(
          id = entityId,
          age = properties.get[Int]("age"),
          gender = properties.get[String]("gender"),
          occupation = properties.get[String]("occupation"),
          zipcode = properties.get[String]("zipcode"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()
  }
}

case class User(id: String, age: Int, gender: String, occupation: String, zipcode: String)

case class Item(id: String, title: String, releaseDate: String, genres: List[String])

case class RatingEvent(user: String, item: String, rating: Double, timestamp: Long)

case class BuyEvent(user: String, item: String, timestamp: Long)

class TrainingData(val users: RDD[(String, User)],
                   val items: RDD[(String, Item)],
                   val ratingEvents: RDD[RatingEvent],
                   val buyEvents: RDD[BuyEvent]
                  ) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
      s"items: [${items.count()} (${items.take(2).toList}...)]" +
      s"ratingEvents: [${ratingEvents.count()}] (${ratingEvents.take(2).toList}...)" +
      s"buyEvents: [${buyEvents.count()}] (${buyEvents.take(2).toList}...)"
  }
}
