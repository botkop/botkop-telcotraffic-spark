package traffic.process

import akka.actor.ActorSystem
import botkop.geo.{GeoUtil, Geofence}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json
import traffic.model.{Celltower, Subscriber}
import traffic.pub.KafkaStreamPublisher
import traffic.util.AppConfig._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object Geofencer extends LazyLogging {

    @volatile var geofences = Geofence.read(geofenceFile)

    // refresh geofences every 5 seconds
    ActorSystem().scheduler.schedule(2 seconds, 5 seconds) {
        geofences = Geofence.read(geofenceFile)
    }

    def detect(unifiedStream: DStream[(Subscriber, Celltower, Map[String, Double])]) = {

        val subscribersInGeofence = unifiedStream.flatMap {
            case (subscriber, celltower, metrics) =>
                geofences
                    .filter(geofence => GeoUtil.containsLocation(celltower.location, geofence.polygon))
                    .map { gf => (subscriber, celltower, gf) }
        } map { // map to json
            case (subscriber, celltower, geofence) =>
                val subJson = Json.stringify(Json.toJson(subscriber))
                val cellJson = Json.stringify(Json.toJson(celltower))
                val gfJson = Json.stringify(Json.toJson(geofence))
                s"""{ "subscriber": $subJson, "celltower": $cellJson, "geofence": $gfJson }"""
        }

        KafkaStreamPublisher.publishStream(geofenceTopic, subscribersInGeofence)

    }

}
