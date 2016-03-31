package traffic.process

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json
import traffic.model._
import traffic.pub.KafkaStreamPublisher
import traffic.util.AppConfig._

object ClusterAnalyser {

    def analyse(unifiedStream: DStream[(Subscriber, Celltower, Map[String, Double])]) = {
        /* initialize model */
        val model: StreamingKMeans = new StreamingKMeans()
            .setK(kMeansK)
            // .setHalfLife(1.5,"batches")
            .setDecayFactor(kMeansDecayFactor)
            .setRandomCenters(kMeansDimensions.length, 0.0)

        /* keep metrics to analyse and transform into vector */
        val vectorStream = unifiedStream.map {
            case (subscriber, celltower, metrics) =>
                val metricsToAnalyse = metrics.filterKeys(kMeansDimensions.contains).values.toArray
                val vector = Vectors.dense(metricsToAnalyse)
                (subscriber, celltower, vector)
        }

        /* train */
        val trainingStream = vectorStream.map { case (_, _, metrics) =>
            metrics
        }
        model.trainOn(trainingStream)

        case class Centroid(prediction: Int, point: Vector)

        /* predict */
        val predictions = vectorStream
            .window(kMeansWindowSize, kMeansSlideSize)
            .map { case (subscriber, celltower, point) =>
                val prediction = model.latestModel.predict(point)
                val centroid = model.latestModel.clusterCenters(prediction)
                val distanceFromCentroid = dist(point, centroid)
                (prediction, ClusterPoint(subscriber, celltower, point, prediction, centroid, distanceFromCentroid))
            }

        val distances = predictions.map { case (prediction, point) => (prediction, List(point.distanceFromCentroid)) }
                .reduceByKey(reduceFunc = (x, y) => x ::: y)

        val thresholds = distances.map { case (prediction, distList: List[Double]) =>
            val (lo, hi) = calculateThresholds(distList)
            (prediction, (lo, hi))
        }

        val outliers = predictions
            .join(thresholds)
            .map { case (prediction, (point, (lo, hi))) =>
                point.outlier = point.distanceFromCentroid < lo || point.distanceFromCentroid > hi
                point
            }

        val jsonStream = outliers.map ( _.toJson)
            .repartition(1)
            .glom()
            .map("""{ "points":  [""" + _.mkString(",") + "] }")

        KafkaStreamPublisher.publishStream(kMeansOutlierTopic, jsonStream)

    }

    def _analyse(unifiedStream: DStream[(Subscriber, Celltower, Map[String, Double])]) = {

        /* initialize model */
        val model: StreamingKMeans = new StreamingKMeans()
            .setK(kMeansK)
            // .setHalfLife(1.5,"batches")
            .setDecayFactor(kMeansDecayFactor)
            .setRandomCenters(kMeansDimensions.length, 0.0)

        /* keep metrics to analyse and transform into vector */
        val vectorStream = unifiedStream.map {
            case (subscriber, celltower, metrics) =>
                val metricsToAnalyse = metrics.filterKeys(kMeansDimensions.contains).values.toArray
                val vector = Vectors.dense(metricsToAnalyse)
                (subscriber, celltower, vector)
        }

        /* train */
        val trainingStream = vectorStream.map { case (_, _, metrics) =>
            metrics
        }
        model.trainOn(trainingStream)

        /* predict */
        val predictions = vectorStream.map {
            case (subscriber, celltower, point) =>
                val prediction = model.latestModel.predict(point)
                (prediction, ClusterPoint(subscriber, celltower, point, prediction))
        }

        val groupedPredictions = predictions.groupByKeyAndWindow(kMeansWindowSize, kMeansSlideSize)

        val groupedPredictionsWithOutlier = groupedPredictions.map {
            case (prediction, listOfPoints) =>

                val centroid = model.latestModel.clusterCenters(prediction)

                // calculate distance from centroid
                listOfPoints.foreach { p => p.distanceFromCentroid = dist(centroid, p.point) }

                // check for outliers
                if (listOfPoints.size > 4) {
                    val (lo, hi) = calculateThresholds[ClusterPoint](listOfPoints, _.distanceFromCentroid)
                    listOfPoints.foreach { p =>
                        p.outlier = p.distanceFromCentroid < lo || p.distanceFromCentroid > hi
                    }
                }

                (prediction, centroid, listOfPoints)
        }

        groupedPredictionsWithOutlier.cache()

        // jsonify
        val jsonStream = groupedPredictionsWithOutlier.map {
            case (prediction, centroid, points) =>
                val pointsJson = s""" "points": [${points.map(_.toJson).mkString(",")}]"""
                val centroidJson = s""" "centroid": ${Json.stringify(Json.toJson(centroid.toArray))}"""
                val dimensionJson = s""" "dimensions": ${Json.stringify(Json.toJson(kMeansDimensions))}"""
                val numPointsJson = s""" "num": ${points.size}"""
                s"""{ "prediction": $prediction,$centroidJson,$dimensionJson,$numPointsJson,$pointsJson }"""
        }

        KafkaStreamPublisher.publishStream(kMeansOutlierTopic, jsonStream)

        /* grouping by subscriber

        val t = groupedPredictionsWithOutlier.flatMap {
            case (prediction, centroid, points) =>
                points
                    .groupBy(_.subscriber)
                    .map {
                        case (subscriber, subscriberPoints) =>
                            val stripped = subscriberPoints.map {
                                case (point) =>
                                    (point.celltower, point.distanceFromCentroid, point.point, point.outlier)
                            } //.toList
                            (prediction, centroid, subscriber, stripped)
                    }
        }

        AppUtil.printStream(t)
        */

    }

    def calculateThresholds(dl: List[Double]) = {
        if (dl.size <= 4) {
            (Double.MinValue, Double.MaxValue)
        } else {
            val v = dl.sortWith(_ < _)
            val q1 = v(v.length / 4)
            val q3 = v(v.length / 4 * 3)
            val iqr = q3 - q1
            val threshold = iqr * 1.5
            val lo = q1 - threshold
            val hi = q3 + threshold
            (lo, hi)
        }
    }

    /* calculate interquartile range and thresholds */
    def calculateThresholds[T](it: Iterable[T], c: (T) => Double) = {
        require(it.size > 4)
        val v = it.toList.sortWith(c(_) < c(_))
        val q1 = c(v(v.length / 4))
        val q3 = c(v(v.length / 4 * 3))
        val iqr = q3 - q1
        val threshold = iqr * 1.5
        val lo = q1 - threshold
        val hi = q3 + threshold
        (lo, hi)
    }

    /* euclidian distance between 2 vectors */
    def dist(v1: Vector, v2: Vector) = math.sqrt(
        v1.toArray.zip(v2.toArray).map(p => p._1 - p._2).map(d => d * d).sum
    )

}
