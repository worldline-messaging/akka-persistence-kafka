package akka.persistence

import scala.jdk.CollectionConverters._

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

package object kafka {
  def journalTopic(persistenceId: String): String =
    persistenceId.replaceAll("[^\\w\\._-]", "_")

  def configToProperties(config: Config, extra: Map[String, String] = Map.empty): Map[String, String] = {

    config.entrySet.asScala.map { entry =>
      entry.getKey -> entry.getValue.unwrapped.toString
    }.toMap ++ extra

  }

  def sendFuture[K, V](p: KafkaProducer[K, V], rec: ProducerRecord[K, V]): Future[Unit] = {
    val promise = Promise[Unit]()
    p.send(
      rec,
      (_: RecordMetadata, exception: Exception) ⇒ {
        if (exception == null) {
          promise.complete(Success())
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
