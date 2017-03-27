package akka.persistence.kafka

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class StickyPartitioner() extends Partitioner {

  var partition = 0

  override def close(): Unit = {}

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = partition

  override def configure(configs: util.Map[String, _]): Unit = {
    partition = Integer.parseInt(configs.get("partition").toString)
  }
}

