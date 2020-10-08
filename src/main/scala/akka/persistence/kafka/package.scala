package akka.persistence

import scala.collection.JavaConverters._

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

package object kafka {
  def journalTopic(persistenceId: String): String =
    persistenceId.replaceAll("[^\\w\\._-]", "_")

  def sequenceTopic(persistenceId: String): String =
    s"sequence-${journalTopic(persistenceId)}"

  def configToProperties(config: Config, extra: Map[String, String] = Map.empty): Map[String, String] = {

    config.entrySet.asScala.map { entry =>
      entry.getKey -> entry.getValue.unwrapped.toString
    }.toMap ++ extra

  }

  def sendFuture[K, V](p: KafkaProducer[K, V], rec: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    p.send(
      rec,
      (rm: RecordMetadata, exception: Exception) ⇒ {
        if (exception == null) {
          promise.complete(Success(rm))
          ()
        } else {
          promise.complete(Failure(exception))
          ()
        }
      }
    )
    promise.future
  }
}
