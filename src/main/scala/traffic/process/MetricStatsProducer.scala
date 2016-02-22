package traffic.process

import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json
import traffic.util.AppConfig._
import traffic.model.{Celltower, MetricStats, Subscriber}
import traffic.pub.KafkaStreamPublisher

object MetricStatsProducer extends Serializable {

    def produce(unifiedStream: DStream[(Subscriber, Celltower, Map[String, Double])]) = {
        produceSubscriberMetricStats(unifiedStream)
        produceCelltowerMetricStats(unifiedStream)
    }

    def produceSubscriberMetricStats(unifiedStream: DStream[(Subscriber, Celltower, Map[String, Double])]) = {

        /* calculate metric stats by subscriber */
        val subscriberStats = unifiedStream map { case (subscriber, _, metrics) =>
            (subscriber, MetricStats(metrics))
        } reduceByKeyAndWindow(
            reduceFunc = { (x, y) => x.merge(y) },
            windowDuration = metricsWindowSize,
            slideDuration = metricsSlideSize
        )

        /* transform to json */
        val jsonSubscriberStats = subscriberStats map { case (subscriber, stats) =>
            val json = Json.stringify(Json.toJson(subscriber))
            s"""{ "subscriber":$json, "stats":${stats.toJson} }"""
        }

        /* publish */
        KafkaStreamPublisher.publishStream(subscriberStatsTopic, jsonSubscriberStats)

    }

    def produceCelltowerMetricStats(unifiedStream: DStream[(Subscriber, Celltower, Map[String, Double])]) = {

        /* calculate metric stats by celltower */
        val celltowerStats = unifiedStream map { case (_, celltower, metrics) =>
            (celltower, MetricStats(metrics))
        } reduceByKeyAndWindow (
            reduceFunc = { (x, y) => x.merge(y) },
            windowDuration = metricsWindowSize,
            slideDuration = metricsSlideSize
        )

        /* transform to json */
        val jsonCelltowerStats = celltowerStats map { case (celltower, stats) =>
            val json = Json.stringify(Json.toJson(celltower))
            s"""{ "celltower":$json, "stats":${stats.toJson} }"""
        }

        /* publish */
        KafkaStreamPublisher.publishStream(celltowerStatsTopic, jsonCelltowerStats)

    }


}
