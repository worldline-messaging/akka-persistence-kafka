package akka.persistence.kafka.journal

import akka.persistence.kafka._
import com.typesafe.config.Config
import kafka.utils._
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaJournalConfig(config: Config) extends MetadataConsumerConfig(config) {
  val writerTimeoutMs: Long = config.getLong("writer-timeout-ms")

  val pluginDispatcher: String =
    config.getString("plugin-dispatcher")

  val writeConcurrency: Int =
    config.getInt("write-concurrency")

  val failedRetries: Int = config.getInt("failed-retry")

  val eventTopicMapper: EventTopicMapper =
    CoreUtils.createObject[EventTopicMapper](config.getString("event.producer.topic.mapper.class"))

  def journalProducerConfig(): Map[String, Object] =
    configToProperties(
      config.getConfig("producer"),
      Map(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   → "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG → "org.apache.kafka.common.serialization.ByteArraySerializer"
      )
    )

  def eventProducerConfig(): Map[String, Object] =
    configToProperties(
      config.getConfig("event.producer"),
      Map(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   → "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG → "org.apache.kafka.common.serialization.ByteArraySerializer"
      )
    ) - "topic.mapper.class"
}
