package traffic.util

import org.apache.spark.streaming.dstream.DStream

object AppUtil {

    /* print stream contents - for debugging only */
    def printStream[T](stream: DStream[T]) = {
        stream.foreachRDD { rdd => rdd.take(5).foreach(println) }
    }

}
