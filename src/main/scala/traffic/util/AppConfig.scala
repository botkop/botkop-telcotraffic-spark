package traffic.util

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.streaming.Milliseconds

import scala.collection.JavaConversions._

case object AppConfig {
    val conf: Config = ConfigFactory.load()
    val checkpoint = conf.getString("spark.checkpoint")
    val batchSize = Milliseconds(conf.getLong("spark.batch.size"))

    /* kafka - consumer */
    val quorum = conf.getString("kafka.consumer.properties.quorum")
    val groupId = conf.getString("kafka.consumer.properties.group.id")

    val attachTopic = conf.getString("kafka.consumer.topics.attach")
    val celltowerTopic = conf.getString("kafka.consumer.topics.celltower")

    /* kafka - producer */
    val kafkaProducerProps = new Properties()
    kafkaProducerProps.put("bootstrap.servers", conf.getString("kafka.producer.properties.bootstrap.servers"))
    kafkaProducerProps.put("acks", conf.getString("kafka.producer.properties.acks"))
    kafkaProducerProps.put("key.serializer", conf.getString("kafka.producer.properties.key.serializer"))
    kafkaProducerProps.put("value.serializer", conf.getString("kafka.producer.properties.value.serializer"))

    val celltowerStatsTopic = conf.getString("kafka.producer.topics.celltower.stats")
    val subscriberStatsTopic = conf.getString("kafka.producer.topics.subscriber.stats")
    val kMeansCentroidTopic = conf.getString("kafka.producer.topics.kmeans.centroid")

    /* cassandra */
    val cassandraHost = conf.getString("cassandra.connection.host")
    val keyspace = conf.getString("cassandra.keyspace")
    val attachTable = conf.getString("cassandra.tables.attach")

    val metricsWindowSize = Milliseconds(conf.getLong("spark.metrics.window.size"))
    val metricsSlideSize = Milliseconds(conf.getLong("spark.metrics.slide.size"))

    /* k means cluster */
    val kMeansDimensions = conf.getStringList("kmeans.dimensions").toList
    val kMeansK = conf.getInt("kmeans.k")
    val kMeansDecayFactor = conf.getDouble("kmeans.decay.factor")
    val kMeansWindowSize = Milliseconds(conf.getLong("kmeans.window.size"))
    val kMeansSlideSize = Milliseconds(conf.getLong("kmeans.slide.size"))
}
