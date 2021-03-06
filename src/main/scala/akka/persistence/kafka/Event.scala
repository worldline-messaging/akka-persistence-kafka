package akka.persistence.kafka

import akka.actor.ActorSystem
import akka.serialization.{Serialization, SerializationExtension}
import kafka.serializer._

import scala.collection.immutable.Seq

/**
 * Event published to user-defined topics.
 *
 * @param persistenceId Id of the persistent actor that generates event `data`.
 * @param sequenceNr Sequence number of the event.
 * @param data Event data generated by a persistent actor.
 */
case class Event(persistenceId: String, sequenceNr: Long, data: Any)

/**
 * Defines a mapping of events to user-defined topics.
 */
trait EventTopicMapper {
  /**
   * Maps an event to zero or more topics.
   *
   * @param event event to be mapped.
   * @return a sequence of topic names.
   */
  def topicsFor(event: Event): Seq[String]
}

class DefaultEventTopicMapper extends EventTopicMapper {
  def topicsFor(event: Event): Seq[String] = List("events")
}

class EmptyEventTopicMapper extends EventTopicMapper {
  def topicsFor(event: Event): Seq[String] = Nil
}

class EventDecoder(system: ActorSystem) extends Decoder[Event] {
  val serialization: Serialization = SerializationExtension(system)

  def fromBytes(bytes: Array[Byte]): Event =
    serialization.deserialize(bytes, classOf[Event]).get
}
