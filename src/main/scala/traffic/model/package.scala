package traffic

import org.apache.spark.mllib.linalg.Vector
import play.api.libs.json.Json


package object model {

    case class LatLng(lat: Double, lng: Double)
    object LatLng {
        implicit val f = Json.format[LatLng]
    }

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

    case class AttachEvent(bearerId: String, subscriber: Subscriber, topic: String)
    object AttachEvent {
        implicit val f = Json.format[AttachEvent]
    }

    case class CelltowerEvent(celltower: Celltower, bearerId: String, metrics: Map[String, Double], topic: String)
    object CelltowerEvent {
        implicit val f = Json.format[CelltowerEvent]
    }

    case class ClusterPrediction(subscriber: Subscriber,
                                 celltower: Celltower,
                                 point: Vector,
                                 prediction: Int,
                                 center: Vector,
                                 var isOutlier: Boolean = false) {
        def toJson = {
            val subscriberJson = s""" "subscriber": ${Json.stringify(Json.toJson(subscriber))} """
            val celltowerJson = s""" "celltower": ${Json.stringify(Json.toJson(celltower))} """
            val pointJson = s""" "point": ${Json.stringify(Json.toJson(point.toArray))} """
            val predictionJson = s""" "prediction": $prediction """
            val centerJson = s""" "center": ${Json.stringify(Json.toJson(center.toArray))} """
            val distJson = s""" "dist": $dist """
            val outlierJson = s""" "outlier": $isOutlier """

            s"{ $subscriberJson, $celltowerJson, $pointJson, $predictionJson, $centerJson, $distJson, $outlierJson }"
        }

        /* calculate Euclidian distance to center */
        lazy val dist = math.sqrt(center.toArray.zip(point.toArray).
            map(p => p._1 - p._2).
            map(d => d * d).sum)
    }

}


