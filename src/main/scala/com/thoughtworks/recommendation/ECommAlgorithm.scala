package com.thoughtworks.recommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{P2LAlgorithm, Params}
import org.apache.predictionio.data.storage.{BiMap, Event}
import org.apache.predictionio.data.store.LEventStore
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating => MLlibRating}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration

case class ECommAlgorithmParams(appName: String,
                                unseenOnly: Boolean,
                                seenEvents: List[String],
                                similarEvents: List[String],
                                rank: Int,
                                numIterations: Int,
                                lambda: Double,
                                seed: Option[Long]
                               ) extends Params


/**
  *
  * @param item     Product
  * @param features Product Vector
  * @param count    Product sell count
  */
case class ProductModel(item: Item,
                        features: Option[Array[Double]],
                        count: Int)

case class WeightGroup(items: Set[String],
                       weight: Double
                      )

class ECommModel(val rank: Int,
                 val userFeatures: Map[Int, Array[Double]],
                 val productModels: Map[Int, ProductModel],
                 val userStringIntMap: BiMap[String, Int],
                 val itemStringIntMap: BiMap[String, Int],
                 val items: Map[Int, Item]
                ) extends Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
      s" userFeatures: [${userFeatures.size}]" +
      s"(${userFeatures.take(2).toList}...)" +
      s" productModels: [${productModels.size}]" +
      s"(${productModels.take(2).toList}...)" +
      s" userStringIntMap: [${userStringIntMap.size}]" +
      s"(${userStringIntMap.take(2).toString}...)]" +
      s" itemStringIntMap: [${itemStringIntMap.size}]" +
      s"(${itemStringIntMap.take(2).toString}...)]"
  }
}

class ECommAlgorithm(val ap: ECommAlgorithmParams) extends P2LAlgorithm[PreparedData, ECommModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): ECommModel = {
    require(!data.ratingEvents.take(1).isEmpty,
      s"ratingEvents in PreparedData cannot be empty." +
        " Please check if DataSource generates TrainingData" +
        " and Preprator generates PreparedData correctly.")
    require(!data.users.take(1).isEmpty,
      s"users in PreparedData cannot be empty." +
        " Please check if DataSource generates TrainingData" +
        " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
        " Please check if DataSource generates TrainingData" +
        " and Preprator generates PreparedData correctly.")

    val userStringIntMap = BiMap.stringInt(data.users.keys)
    val itemStringIntMap = BiMap.stringInt(data.items.keys)

    val mllibRatings: RDD[MLlibRating] = genMLlibRating(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = data
    )


    require(!mllibRatings.take(1).isEmpty, s"mllibRatings cannot be empty. Please check if your events contain valid user and item ID.")

    val seed = ap.seed.getOrElse(System.nanoTime)

    /*val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed)*/

    val m: MatrixFactorizationModel = ALS.train(
      ratings = mllibRatings,
      rank = ap.rank, iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      seed = seed
    )

    // User Vectors
    val userFeatures = m.userFeatures.collectAsMap.toMap

    val items = data.items.map { case (id, item) => (itemStringIntMap(id), item) }

    // Product Vectors
    val productFeatures: Map[Int, (Item, Option[Array[Double]])] = items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    // User buy behavior
    val popularCount = trainDefault(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = data
    )

    val productModels: Map[Int, ProductModel] = productFeatures
      .map { case (index, (item, features)) => (index, ProductModel(item = item, features = features, count = popularCount.getOrElse(index, 0))) }

    new ECommModel(
      rank = m.rank,
      userFeatures = userFeatures,
      productModels = productModels,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      items = items.collectAsMap().toMap
    )
  }


  def genMLlibRating(userStringIntMap: BiMap[String, Int],
                     itemStringIntMap: BiMap[String, Int],
                     data: PreparedData): RDD[MLlibRating] = {

    val mllibRatings = data.ratingEvents
      .map { r =>
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user} to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item} to Int index.")

        ((uindex, iindex), r.rating, r.timestamp)
      }
      .filter { case ((u, i), v, t) => (u != -1) && (i != -1) }
      .map { case ((u, i), v, t) => MLlibRating(u, i, v) }
      .cache()
    mllibRatings
  }

  def trainDefault(userStringIntMap: BiMap[String, Int],
                   itemStringIntMap: BiMap[String, Int],
                   data: PreparedData): Map[Int, Int] = {
    val buyCountsRDD: RDD[(Int, Int)] = data.buyEvents
      .map { r =>
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user} to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item} to Int index.")

        (uindex, iindex, 1)
      }
      .filter { case (u, i, v) => (u != -1) && (i != -1) }
      .map { case (u, i, v) => (i, 1) }
      .reduceByKey { case (a, b) => a + b }

    buyCountsRDD.collectAsMap.toMap
  }

  override def batchPredict(model: ECommModel, query: RDD[(Long, Query)]): RDD[(Long, PredictedResult)] = super.batchPredict(model, query)

  def predict(model: ECommModel, query: Query): PredictedResult = {
    val userFeatures = model.userFeatures
    val productModels = model.productModels
    //    query.whiteList.map(model.itemStringIntMap.get)
    val whiteList: Option[Set[Int]] = query.whiteList.map(set => set.flatMap(model.itemStringIntMap.get))


    val finalBlackList: Set[Int] = genBlackList(query = query).flatMap(model.itemStringIntMap.get)

    val weights: Map[Int, Double] = (for {
      group <- weightedItems
      item <- group.items
      index <- model.itemStringIntMap.get(item)
    } yield (index, group.weight)).toMap.withDefaultValue(1.0)

    val userFeature: Option[Array[Double]] =
      model.userStringIntMap.get(query.user).flatMap { userIndex => userFeatures.get(userIndex) }

    // 1. If this user can be found, will dot user vector and product vector, if not found, will get latest products the user viewed
    // 2. If the latest products the user viewed can be found, find similar products
    // 3. If the latest products the user viewed can not be found, order these products by sell count
    val topScores: Array[(Int, Double)] = if (userFeature.isDefined) {
      /*val indexScores: Map[Int, Double] = productModels.par
        .map { case (i, pm) =>
          val s = dotProduct(userFeature.get, pm.features.get)
          (i, s)
        }
        .filter(_._2 > 0)
        .seq

      val ord = Ordering.by[(Int, Double), Double](_._2).reverse
      val topScores = getTopN(indexScores, query.num)(ord).toArray

      topScores*/
      predictKnownUser(
        userFeature = userFeature.get,
        productModels = productModels,
        query = query,
        whiteList = whiteList,
        blackList = finalBlackList,
        weights = weights
      )
    } else {
      /*logger.info(s"No prediction for unknown user ${query.user}.")
      Array.empty[(Int, Double)]*/
      logger.info(s"No userFeature found for user ${query.user}.")

      val recentItems: Set[String] = getRecentItems(query)
      val recentList: Set[Int] = recentItems.flatMap(x => model.itemStringIntMap.get(x))

      val recentFeatures: Vector[Array[Double]] = recentList.toVector
        .map { i => productModels.get(i).flatMap { pm => pm.features } }.flatten

      if (recentFeatures.isEmpty) {
        logger.info(s"No features vector for recent items ${recentItems}.")
        predictDefault(
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList,
          weights = weights
        )
      } else {
        predictSimilar(
          recentFeatures = recentFeatures,
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList,
          weights = weights
        )
      }
    }



    val itemScores = topScores.map { case (i, s) =>
      val it = model.items(i)

      new ItemScore(
        item = model.itemIntStringMap(i),
        title = it.title,
        releaseDate = it.releaseDate,
        genres = Constants.GENDER_FIELDS.zip(it.genres).toMap,
        score = s)
    }

    new PredictedResult(itemScores)
  }

  def genBlackList(query: Query): Set[String] = {
    val seenItems: Set[String] = if (ap.unseenOnly) {

      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user,
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some("item")),
          timeout = Duration(200, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events. Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get
        } catch {
          case e: Exception => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[String]()
    }

    val unavailableItems: Set[String] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Set[String]]("items")
      } else {
        Set[String]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableItems event." +
          s" Empty list is used. ${e}")
        Set[String]()
      case e: Exception =>
        logger.error(s"Error when read set unavailableItems event: ${e}")
        throw e
    }

    query.blackList.getOrElse(Set[String]()) ++ seenItems ++ unavailableItems
  }

  def weightedItems: Seq[WeightGroup] = {
    try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "weightedItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Seq[WeightGroup]]("weights")
      } else {
        Nil
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set weightedItems event." +
          s" Empty list is used. ${e}")
        Nil
      case e: Exception =>
        logger.error(s"Error when read set weightedItems event: ${e}")
        throw e
    }
  }

  def getRecentItems(query: Query): Set[String] = {
    // get latest 10 user view item events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "user",
        entityId = query.user,
        eventNames = Some(ap.similarEvents),
        targetEntityType = Some(Some("item")),
        limit = Some(10),
        latest = true,
        timeout = Duration(600, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events. Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e: Exception => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }

  def predictKnownUser(userFeature: Array[Double],
                       productModels: Map[Int, ProductModel],
                       query: Query,
                       whiteList: Option[Set[Int]],
                       blackList: Set[Int],
                       weights: Map[Int, Double]
                      ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par
      .filter { case (i, pm) =>
        pm.features.isDefined &&
          isCandidateItem(i, pm.item, query.genres, whiteList, blackList)
      }
      .map { case (i, pm) =>
        val s = dotProduct(userFeature, pm.features.get)
        val adjustedScore = s * weights(i)
        (i, adjustedScore)
      }
      .filter(_._2 > 0)
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  def predictDefault(productModels: Map[Int, ProductModel],
                     query: Query,
                     whiteList: Option[Set[Int]],
                     blackList: Set[Int],
                     weights: Map[Int, Double]
                    ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par
      .filter { case (i, pm) =>
        isCandidateItem(i, pm.item, query.genres, whiteList, blackList)
      }
      .map { case (i, pm) =>
        val s = pm.count.toDouble
        val adjustedScore = s * weights(i)
        (i, adjustedScore)
      }
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  def predictSimilar(recentFeatures: Vector[Array[Double]],
                     productModels: Map[Int, ProductModel],
                     query: Query,
                     whiteList: Option[Set[Int]],
                     blackList: Set[Int],
                     weights: Map[Int, Double]
                    ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par
      .filter { case (index, pm) =>
        pm.features.isDefined && isCandidateItem(index, pm.item, query.genres, whiteList, blackList)
      }
      .map { case (i, pm) =>
        val s = recentFeatures.map { rf => cosine(rf, pm.features.get) }.reduce(_ + _)
        val adjustedScore = s * weights(i)
        (i, adjustedScore)
      }
      .filter(_._2 > 0)
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  private def getTopN[T](s: Iterable[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.length
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

  private def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.length
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = math.sqrt(n1) * math.sqrt(n2)
    if (n1n2 == 0) 0 else d / n1n2
  }

  private def isCandidateItem(i: Int,
                              item: Item,
                              genres: Option[Set[String]],
                              whiteList: Option[Set[Int]],
                              blackList: Set[Int]
                             ): Boolean = {
    whiteList.map(_.contains(i)).getOrElse(true) & !blackList.contains(i) &
      (genres.map { genre => genre.map(g => Constants.GENDER_FIELDS.indexOf(g)) } match {
        case Some(indexes) => indexes.forall { index => item.genres(index) == 1 }
        case None => true
      })
  }

}
