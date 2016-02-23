package traffic

import com.datastax.spark.connector.streaming._
import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import play.api.libs.json.Json
import traffic.model._
import traffic.process.{ClusterAnalyser, MetricStatsProducer}
import traffic.util.AppConfig._

object TrafficStreamProcessor {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("TrafficStreamProcessor")
            .set("spark.logConf", "true")
            .set("spark.akka.logLifecycleEvents", "true")
            .set("spark.cassandra.connection.host", cassandraHost)

        val ssc = new StreamingContext(sparkConf, batchSize)
        ssc.checkpoint(checkpoint)

        process(ssc)

        val actorSystem = SparkEnv.get.actorSystem

        ssc.start()
        ssc.awaitTermination()
    }

    def process(ssc: StreamingContext): Unit = {

        /* capture the attach stream and transform into attach events */
        val attachStream = KafkaUtils
            .createStream(ssc, quorum, groupId, Map(attachTopic -> 1))
            .map(_._2)
            .map(Json.parse)
            .flatMap(_.asOpt[AttachEvent])

        /* save the attach stream to the database */
        attachStream.saveToCassandra(keyspace, attachTable)

        /* capture the celltower stream and transform into celltower events */
        val celltowerStream = KafkaUtils
            .createStream(ssc, quorum, groupId, Map(celltowerTopic -> 1))
            .map(_._2)
            .map(Json.parse)
            .flatMap(_.asOpt[CelltowerEvent])

        /* join the celltower stream with the persisted attach stream (on bearerId) */
        val unifiedStream = celltowerStream
            .joinWithCassandraTable[AttachEvent](keyspace, attachTable)
            .map { case (celltowerEvent, attachEvent ) =>
                (attachEvent.subscriber, celltowerEvent.celltower, celltowerEvent.metrics)
            }

        unifiedStream.cache

        MetricStatsProducer.produce(unifiedStream)

        ClusterAnalyser.analyse(unifiedStream)

    }

}

