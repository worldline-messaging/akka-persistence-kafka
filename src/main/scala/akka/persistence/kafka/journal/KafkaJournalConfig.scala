package akka.persistence.kafka.journal

import java.util.Properties

import scala.concurrent.duration._

import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka._
import com.typesafe.config.Config
import kafka.utils._
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaJournalConfig(config: Config) extends MetadataConsumerConfig(config) {
  val pluginDispatcher: String =
    config.getString("plugin-dispatcher")

  val writeConcurrency: Int = config.getInt("write-concurrency")

  val requestTimeoutInMs:FiniteDuration = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG).millis

  val eventTopicMapper: EventTopicMapper =
    CoreUtils.createObject[EventTopicMapper](config.getString("event.producer.topic.mapper.class"))

  def journalProducerConfig(brokers: List[Broker]): Properties =
    configToProperties(config.getConfig("producer"),
      Map("metadata.broker.list" -> Broker.toString(brokers), "partition" -> config.getString("partition"),"key.serializer" -> config.getString("key.serializer")))

  def eventProducerConfig(brokers: List[Broker]): Properties =
    configToProperties(config.getConfig("event.producer"),
      Map("metadata.broker.list" -> Broker.toString(brokers),"key.serializer" -> config.getString("key.serializer")))
}
