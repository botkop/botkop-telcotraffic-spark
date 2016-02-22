package traffic.pub

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream
import traffic.util.AppConfig._

object KafkaStreamPublisher extends Serializable {

    def publishStream(topic: String, stream: DStream[String]) = {
        stream.foreachRDD { rdd =>
            rdd.cache()
            rdd.foreachPartition { partition =>

                val producer = new KafkaProducer[String, String](kafkaProducerProps)

                partition.foreach { event =>
                    val message = new ProducerRecord[String, String](topic, event)
                    producer.send(message)
                }

                producer.close()
            }
            rdd.unpersist()
        }
    }

}

