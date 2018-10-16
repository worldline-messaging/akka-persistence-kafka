package akka.persistence.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.blocking

trait MetadataConsumer {

  def nextOffsetFor(config: Map[String, Object], topic: String, partition: Int): Long = blocking {
    val tp       = new TopicPartition(topic, partition)
    val consumer = new KafkaConsumer[String, Array[Byte]](config.asJava)
    try {
      consumer.assign(List(tp).asJava)
      consumer.endOffsets(List(tp).asJava).get(tp)
    } finally {
      consumer.close()
    }
  }

}
