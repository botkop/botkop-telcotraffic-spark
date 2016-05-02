package traffic.process

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.dstream.DStream
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
        val vectorStream = unifiedStream
            .map {
            case (subscriber, celltower, metrics) =>
                val metricsToAnalyse = metrics.filterKeys(kMeansDimensions.contains).values.toArray
                val vector = Vectors.dense(metricsToAnalyse)
                (subscriber, celltower, vector)
        }

        val trainingStream = vectorStream.map { case (_, _, metrics) =>
            metrics
        }

        model.trainOn(trainingStream)

        val predictions = vectorStream
            .window(kMeansWindowSize, kMeansSlideSize)
            .map { case (subscriber, celltower, point) =>
                val prediction = model.latestModel.predict(point)
                val centroid = model.latestModel.clusterCenters(prediction)
                val distanceFromCentroid = dist(point, centroid)
                (prediction, ClusterPoint(subscriber, celltower, point, prediction, centroid, distanceFromCentroid))
            }

        val distances = predictions
            .map { case (prediction, point) => (prediction, List(point.distanceFromCentroid)) }
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
