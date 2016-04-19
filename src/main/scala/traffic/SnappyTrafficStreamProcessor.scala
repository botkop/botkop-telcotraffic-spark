package traffic

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import play.api.libs.json.Json
import traffic.model._
import traffic.process.{ClusterAnalyser, Geofencer, MetricStatsProducer}
import traffic.util.AppConfig._

object SnappyTrafficStreamProcessor {

  // keep the two different for now due to an issue in joining snappy tables
  // with spark partitioning (spark will skip partitioning table if it matches)

  /** number of partitions in the snappydata table */
  private val PARTITIONS = 11

  /**
   * The value of "spark.default.parallelism" i.e. number of partitions
   * used for joins (on the fly re-partitioning). Setting this to zero
   * will disable it and fallback to the default in Spark.
   */
  private val PARALLELISM = 8

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SnappyTrafficStreamProcessor")
      .set("spark.logConf", "true")
      .set("spark.akka.logLifecycleEvents", "true")
    // to run against an embedded SnappyData cluster
    // .set("snappydata.store.locators", "localhost[10334]")

    if (PARALLELISM > 0) {
      sparkConf.set("spark.default.parallelism", PARALLELISM.toString)
    }

    val snsc = new SnappyStreamingContext(sparkConf, batchSize)

    process(snsc)

    snsc.start()
    snsc.awaitTermination()
  }

  def process(snsc: SnappyStreamingContext): Unit = {

    /* capture the attach stream and transform into attach events */
    val attachStream = KafkaUtils
      .createStream(snsc, quorum, groupId, Map(attachTopic -> 1))
      .map(_._2)
      .map(Json.parse)
      .flatMap(_.asOpt[AttachEvent])

    /* save the attach stream to the database */
    val sc = snsc.snappyContext
    sc.sql(s"set spark.sql.shuffle.partitions=$PARALLELISM")
    val attachDFStream = snsc.createSchemaDStream(attachStream)

    sc.dropTable(attachTable, ifExists = true)
    sc.createTable(attachTable, "column", attachDFStream.schema,
      Map("buckets" -> PARTITIONS.toString))
    attachDFStream.foreachDataFrame(_.write.insertInto(attachTable))

    /* capture the celltower stream and transform into celltower events */
    val celltowerStream = KafkaUtils
      .createStream(snsc, quorum, groupId, Map(celltowerTopic -> 1))
      .map(_._2)
      .map(Json.parse)
      .flatMap(_.asOpt[CelltowerEvent])

    /* join the celltower stream with the persisted attach stream (on bearerId) */
    val unifiedStream = celltowerStream.transform { rdd =>
      val df = sc.table(attachTable)
      val subscriberArity = df.schema(1).dataType.asInstanceOf[StructType].length
      val dfRdd = df.queryExecution.toRdd.mapPartitions { iter =>
        iter.map { row =>
          val bearerId = row.getString(0)
          val c1 = row.getStruct(1, subscriberArity)
          val subscriber = Subscriber(c1.getInt(0), c1.getString(1),
            c1.getString(2), c1.getString(3), c1.getString(4), c1.getString(5),
            c1.getString(6), c1.getString(7), c1.getString(8), c1.getString(9))
          (bearerId, subscriber)
        }
      }
      rdd.map(ev => (ev.bearerId, ev)).join(dfRdd)
        .map(ev => (ev._2._2, ev._2._1.celltower, ev._2._1.metrics))
    }

    unifiedStream.cache()

    MetricStatsProducer.produce(unifiedStream)

    ClusterAnalyser.analyse(unifiedStream)

    Geofencer.detect(unifiedStream)
  }
}
