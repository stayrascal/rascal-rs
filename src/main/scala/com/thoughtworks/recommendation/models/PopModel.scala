package com.thoughtworks.recommendation.models

import grizzled.slf4j.Logger
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Interval}

object PopModel {
  @transient lazy val logger = Logger[this.type]

  def eventsRDD(appName: String, eventNames: List[String], interval: Interval)(implicit sc: SparkContext): RDD[Event] = {
    PEventStore.find(
      appName = appName,
      startTime = Some(interval.getStart),
      untilTime = Some(interval.getEnd),
      eventNames = Some(eventNames)
    )(sc)
  }

  def calcPopular(appName: String,
                  eventNames: List[String] = List.empty,
                  interval: Interval)(implicit sc: SparkContext): Option[RDD[(String, Float)]] = {
    val events = eventsRDD(appName, eventNames, interval)
    val retVal = events.map { event => (event.targetEntityId, event.event) }
      .groupByKey()
      .map { case (itemID, itEvents) => (itemID.get, itEvents.size.toFloat) }
      .reduceByKey(_ + _)
    if (!retVal.isEmpty()) Some(retVal) else None
  }


  def calcTrending(appName: String,
                   eventNames: List[String] = List.empty,
                   interval: Interval)(implicit sc: SparkContext): Option[RDD[(String, Float)]] = {
    val olderInterval = new Interval(interval.getStart, interval.getStart.plusMillis((interval.toDurationMillis / 2).toInt))
    val newerInterval = new Interval(interval.getStart.plusMillis((interval.toDurationMillis / 2).toInt), interval.getEnd)
    val intervalMillis = interval.toDurationMillis
    val olderPopRDD = calcPopular(appName, eventNames, olderInterval)
    if (olderPopRDD.nonEmpty) {
      val newerPopRDD = calcPopular(appName, eventNames, newerInterval)
      if (newerPopRDD.nonEmpty) {
        val retVal = newerPopRDD.get.join(olderPopRDD.get).map { case (item, (newerScore, olderScore)) =>
          val velocity = newerScore - olderScore
          (item, velocity)
        }
        if (!retVal.isEmpty()) Some(retVal) else None
      } else None
    } else None
  }

  def calcHot(appName: String,
              eventNames: List[String] = List.empty,
              interval: Interval)(implicit sc: SparkContext): Option[RDD[(String, Float)]] = {
    val olderInterval = new Interval(interval.getStart, interval.getStart.plusMillis((interval.toDurationMillis / 3).toInt))
    val middleInterval = new Interval(olderInterval.getEnd, interval.getStart.plusMillis((interval.toDurationMillis / 3).toInt))
    val newerInterval = new Interval(middleInterval.getEnd, interval.getEnd)

    val olderPopRDD = calcPopular(appName, eventNames, olderInterval)
    if (olderPopRDD.nonEmpty) {
      val middlePopRDD = calcPopular(appName, eventNames, middleInterval)
      if (middlePopRDD.nonEmpty) {
        val newerPopRDD = calcPopular(appName, eventNames, newerInterval)
        if (newerPopRDD.nonEmpty) {
          val newVelocityRDD = newerPopRDD.get.join(middlePopRDD.get).map {
            case (item, (newerScore, olderScore)) => {
              val velocity = newerScore - olderScore
              (item, velocity)
            }
          }
          val oldVelocityRDD = middlePopRDD.get.join(olderPopRDD.get).map {
            case (item, (newerScore, olderScore)) => {
              val velocity = newerScore - olderScore
              (item, velocity)
            }
          }
          Some(newVelocityRDD.join(oldVelocityRDD).map {
            case (item, (newVelocity, oldVelocity)) =>
              val acceleration = newVelocity - oldVelocity
              (item, acceleration)
          })
        } else None
      } else None
    } else None
  }

  def calc(modelName: Option[String] = None,
           eventNames: List[String],
           appName: String,
           duration: Int,
           endDateOption: Option[String] = None)(implicit sc: SparkContext): Option[RDD[(String, Float)]] = {
    val endDate = if (endDateOption.isEmpty) DateTime.now() else {
      try {
        ISODateTimeFormat.dateTimeParser().parseDateTime(endDateOption.get)
      } catch {
        case e: IllegalArgumentException =>
          logger.warn(s"Bad endDate for popModel: ${endDateOption.get} using 'now'")
          DateTime.now()
      }
    }

    modelName match {
      case Some("popular") => calcPopular(appName, eventNames, new Interval(endDate.minusSeconds(duration), endDate))
      case Some("trending") => calcTrending(appName, eventNames, new Interval(endDate.minusSeconds(duration), endDate))
      case Some("hot") => calcHot(appName, eventNames, new Interval(endDate.minusSeconds(duration), endDate))
      case _ => None
    }
  }
}
