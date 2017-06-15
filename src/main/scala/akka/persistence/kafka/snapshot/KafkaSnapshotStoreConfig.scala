package akka.persistence.kafka.snapshot

import java.util.Properties

import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka._
import com.typesafe.config.Config



class KafkaSnapshotStoreConfig(config: Config) extends MetadataConsumerConfig(config) {
  val prefix: String =
    config.getString("prefix")

  val ignoreOrphan: Boolean =
    config.getBoolean("ignore-orphan")

  def producerConfig(brokers: List[Broker]): Properties =
    configToProperties(config.getConfig("producer"),
      Map("metadata.broker.list" -> Broker.toString(brokers), "partition" -> config.getString("partition")))
}
