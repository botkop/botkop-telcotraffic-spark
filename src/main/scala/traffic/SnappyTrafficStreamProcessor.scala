package traffic

import botkop.geo.LatLng
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import play.api.libs.json.Json
import traffic.model._
import traffic.process.{ClusterAnalyser, Geofencer, MetricStatsProducer}
import traffic.util.AppConfig._

object SnappyTrafficStreamProcessor {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SnappyTrafficStreamProcessor")
      .set("spark.logConf", "true")
      .set("spark.akka.logLifecycleEvents", "true")
      // to run against an embedded SnappyData cluster
      // .set("snappydata.store.locators", "localhost[10334]")

    val snsc = new SnappyStreamingContext(sparkConf, batchSize)

    snsc.checkpoint(checkpoint)

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
    sc.sql("set spark.sql.shuffle.partitions=8")
    val attachDFStream = snsc.createSchemaDStream(attachStream)
    sc.dropTable(attachTable, ifExists = true)
    sc.createTable(attachTable, "column", attachDFStream.schema,
      Map.empty[String, String])
    attachDFStream.foreachDataFrame(_.write.insertInto(attachTable))

    /* capture the celltower stream and transform into celltower events */
    val celltowerStream = KafkaUtils
      .createStream(snsc, quorum, groupId, Map(celltowerTopic -> 1))
      .map(_._2)
      .map(Json.parse)
      .flatMap(_.asOpt[CelltowerEvent])

    /* join the celltower stream with the persisted attach stream (on bearerId) */
    val unifiedStream = celltowerStream.transform { rdd =>
      val df = sc.createDataFrame(rdd)
      val joinDf = df.join(sc.table(attachTable), "bearerId").select(
        "subscriber", "celltower", "metrics")
      val schema = joinDf.schema
      val subscriberArity = schema(0).dataType.asInstanceOf[StructType].length
      val celltowerArity = schema(1).dataType.asInstanceOf[StructType].length
      val metricsType = schema(2).dataType
      joinDf.queryExecution.toRdd.mapPartitions { iter =>
        val method = CatalystTypeConverters.getClass.getDeclaredMethod(
          "createToScalaConverter", classOf[DataType])
        method.setAccessible(true)
        val converter = method.invoke(
          CatalystTypeConverters, metricsType).asInstanceOf[Any => Any]
        iter.map { row =>
          val c1 = row.getStruct(0, subscriberArity)
          val c2 = row.getStruct(1, celltowerArity)
          val c2_4 = c2.getStruct(4, 2)
          val metrics = converter(row.getMap(2))
          val subscriber = Subscriber(c1.getInt(0), c1.getString(1),
            c1.getString(2), c1.getString(3), c1.getString(4), c1.getString(5),
            c1.getString(6), c1.getString(7), c1.getString(8), c1.getString(9))
          val cellTower = Celltower(c2.getInt(0), c2.getInt(1), c2.getInt(2),
            c2.getInt(3), LatLng(c2_4.getDouble(0), c2_4.getDouble(1)))
          (subscriber, cellTower, metrics.asInstanceOf[Map[String, Double]])
        }
      }
    }

    unifiedStream.cache()

    MetricStatsProducer.produce(unifiedStream)

    ClusterAnalyser.analyse(unifiedStream)

    Geofencer.detect(unifiedStream)
  }
}
