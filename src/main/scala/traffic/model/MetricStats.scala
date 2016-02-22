package traffic.model

import org.apache.spark.util.StatCounter

class MetricStats(private var stats: Map[String, StatCounter]) extends Serializable {

    def merge(other: MetricStats): MetricStats = {
        stats map { case (label, stat) =>
            stat.merge(other.stats(label))
        }
        this
    }

    override def toString = stats.toString()

    def statCounterToJson(sc: StatCounter) =
        """{ "count":%d, "mean":%f, "stdev":%f, "max":%f, "min":%f }"""
            .format(sc.count, sc.mean, sc.stdev, sc.max, sc.min)

    def toJson: String =
        "{" +
        stats.map { case (k, sc) => s""" "$k": ${statCounterToJson(sc)} """ }.mkString(",") +
        "}"
}

object MetricStats extends Serializable {

    def apply(x: Map[String, Double]) = {
        var stats = Map.empty[String, StatCounter]
        x.foreach { case (k, v) => stats += k -> new StatCounter().merge(v)}
        new MetricStats(stats)
    }

}
