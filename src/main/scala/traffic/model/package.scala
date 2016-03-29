package traffic

import botkop.geo.LatLng
import org.apache.spark.mllib.linalg.Vector
import play.api.libs.json.Json


package object model {

    case class Celltower(mcc: Int, mnc: Int, cell: Int, area: Int, location: LatLng)
    object Celltower {
        implicit val f = Json.format[Celltower]
    }

    case class Subscriber(id: Int,
                          imsi: String,
                          msisdn: String,
                          imei: String,
                          lastName: String,
                          firstName: String,
                          address: String,
                          city: String,
                          zip: String,
                          country: String)
    object Subscriber {
        implicit val f = Json.format[Subscriber]
    }

    case class AttachEvent(bearerId: String, subscriber: Subscriber, topic: String, ts: Long)
    object AttachEvent {
        implicit val f = Json.format[AttachEvent]
    }

    case class CelltowerEvent(celltower: Celltower, bearerId: String, metrics: Map[String, Double], topic: String, ts: Long)
    object CelltowerEvent {
        implicit val f = Json.format[CelltowerEvent]
    }

    case class ClusterPoint(subscriber: Subscriber,
                            celltower: Celltower,
                            point: Vector,
                            var distanceFromCentroid: Double = 0.0,
                            var outlier: Boolean = false) {
        def toJson = {
            val subJson = s""" "subscriber": ${Json.stringify(Json.toJson(subscriber))} """
            val cellJson = s""" "celltower": ${Json.stringify(Json.toJson(celltower))} """
            val pointJson = s""" "point": ${Json.stringify(Json.toJson(point.toArray))} """
            val distJson = s""" "distance": $distanceFromCentroid """
            val outJson = s""" "outlier": $outlier """

            s"""{ $subJson, $cellJson, $pointJson, $distJson, $outJson }"""
        }
    }

}

