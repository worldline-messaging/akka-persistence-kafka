package akka.persistence.kafka.snapshot

import java.time

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.actor._
import akka.pattern.ask
import akka.persistence._
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.kafka._
import akka.serialization.{Serialization, SerializationExtension}
import akka.util.Timeout
import akka.persistence.kafka.journal.KafkaJournalProtocol._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._
import scala.io.StdIn

object KafkaSnapshotStoreViewer extends App {
  case class ViewSnapshots (persistenceId:String)

  if (args.length == 0) {
    println("dude, i need at least one parameter")
  } else {
    val system = ActorSystem("snapshot-store-viewer")
    val actor   = system.actorOf(Props(new KafkaSnapshotStoreViewer()))

    actor ! ViewSnapshots(args(0))

    StdIn.readChar()
  }

}
class KafkaSnapshotStoreViewer extends Actor with MetadataConsumer with ActorLogging {
  import KafkaSnapshotStoreViewer._

  import context.dispatcher

  val extension: Persistence = Persistence(context.system)

  val serialization: Serialization = SerializationExtension(context.system)

  val config = new KafkaSnapshotStoreConfig(context.system.settings.config.getConfig("kafka-snapshot-store"))

  private def highestJournalSequenceNr(persistenceId: String): Future[Long] = {
    val journal = extension.journalFor(null)
    implicit val timeout: Timeout = Timeout(50.seconds)
    val res = journal ? ReadHighestSequenceNr(0L, persistenceId, self)
    res.flatMap {
      case ReadHighestSequenceNrSuccess(snr) => Future.successful(snr+1)
      case ReadHighestSequenceNrFailure(err) => Future.failed(err)
    }
  }

  private def snapshot(topic: String, offset: Long): KafkaSnapshot = {
    val iter = new MessageIterator(config.snapshotConsumerConfig, topic, config.partition, offset, time.Duration.ofMillis(config.pollTimeOut))
    if(!iter.hasNext && offset>0)
      log.warning(s"Strange: Offset is not 0 ($offset), But iterator is empty. Perhaps you should increase the poll-timeout parameter (${config.pollTimeOut} ms)")
    try { serialization.deserialize(iter.next().value(), classOf[KafkaSnapshot]).get } finally { iter.close() }
  }

  private def snapshotTopic(persistenceId: String): String =
    s"${config.prefixSnapshot}${journalTopic(persistenceId)}"

  def load(topic: String, matcher: KafkaSnapshot => Boolean): Unit = {
    val offset = nextOffsetFor(config.snapshotConsumerConfig, topic, config.partition)

    @annotation.tailrec
    def load(topic: String, offset: Long): Option[KafkaSnapshot] = {
      println(s"Load $topic with $offset")
      if (offset < 0) None else {
        val s = snapshot(topic, offset)
        if (matcher(s)) Some(s) else load(topic, offset - 1)
      }
    }

    println("Snaphot="+load(topic, offset - 1))
  }

  override def receive: Receive = {
    case ViewSnapshots(persistenceId) => {
      val highest = Await.result(highestJournalSequenceNr(persistenceId),5.seconds)
      println(s"highest=$highest")
      val criteria = SnapshotSelectionCriteria.Latest
      val adjusted = if (
        highest < criteria.maxSequenceNr &&
        highest > 0L) criteria.copy(maxSequenceNr = highest) else criteria
      println(s"adjusted=$adjusted")
      def matcher(snapshot: KafkaSnapshot): Boolean = snapshot.matches(adjusted)

      load(snapshotTopic(persistenceId),matcher)
    }
  }
}

class KafkaSnapshotStore extends SnapshotStore with MetadataConsumer with ActorLogging {
  import context.dispatcher

  val extension: Persistence = Persistence(context.system)

  type RangeDeletions = Map[String, SnapshotSelectionCriteria]
  type SingleDeletions = Map[String, List[SnapshotMetadata]]

  val serialization: Serialization = SerializationExtension(context.system)
  val config = new KafkaSnapshotStoreConfig(context.system.settings.config.getConfig("kafka-snapshot-store"))

  val snapshotProducer = new KafkaProducer[String, Array[Byte]](config.producerConfig().asJava)

  override def postStop(): Unit = {
    super.postStop()
  }

  // Transient deletions only to pass TCK (persistent not supported)
  var rangeDeletions: RangeDeletions = Map.empty.withDefaultValue(SnapshotSelectionCriteria.None)
  var singleDeletions: SingleDeletions = Map.empty.withDefaultValue(Nil)

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful {
    rangeDeletions += (persistenceId -> criteria)
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful {
    singleDeletions.get(metadata.persistenceId) match {
      case Some(dels) => singleDeletions += (metadata.persistenceId -> (metadata :: dels))
      case None       => singleDeletions += (metadata.persistenceId -> List(metadata))
    }
  }

  private def saveSequenceOffset(metadata: SnapshotMetadata):Future[Long]= {
    highestJournalSequenceNr(metadata.persistenceId).flatMap { highest =>
      val sequenceBytes = serialization.serialize(KafkaSequenceOffset(metadata.sequenceNr,highest)).get
      val snapshotMessage = new ProducerRecord[String, Array[Byte]](sequenceTopic(metadata.persistenceId), "static", sequenceBytes)
      // TODO: take a producer from a pool ?
      sendFuture(snapshotProducer, snapshotMessage).map(_ => highest)
    }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    saveSequenceOffset(metadata).flatMap {highest =>
      val snapshotBytes = serialization.serialize(KafkaSnapshot(metadata, snapshot)).get
      val snapshotMessage = new ProducerRecord[String, Array[Byte]](snapshotTopic(metadata.persistenceId), "static", snapshotBytes)
      // TODO: take a producer from a pool ?
      sendFuture(snapshotProducer, snapshotMessage).map{_ =>
        println(s"Snaphot ${metadata.persistenceId} with snr ${metadata.sequenceNr} at offset ${highest}")
      }
    }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val singleDeletions = this.singleDeletions
    val rangeDeletions = this.rangeDeletions

    for {
      highest <- if (config.ignoreOrphan) highestJournalSequenceNr(persistenceId) else Future.successful(Long.MaxValue)
      adjusted = if (config.ignoreOrphan &&
        highest < criteria.maxSequenceNr &&
        highest > 0L) criteria.copy(maxSequenceNr = highest) else criteria
      // highest  <- Future.successful(Long.MaxValue)
      // adjusted = criteria
      snapshot <- Future.successful {
        val topic = snapshotTopic(persistenceId)
        // if timestamp was unset on delete, matches only on same sequence nr
        def matcher(snapshot: KafkaSnapshot): Boolean = snapshot.matches(adjusted) &&
          !snapshot.matches(rangeDeletions(persistenceId)) &&
          !singleDeletions(persistenceId).contains(snapshot.metadata) &&
          !singleDeletions(persistenceId).filter(_.timestamp == 0L).map(_.sequenceNr).contains(snapshot.metadata.sequenceNr)

        load(topic, matcher).map(s => SelectedSnapshot(s.metadata, s.snapshot))
      }
    } yield snapshot
  }

  def load(topic: String, matcher: KafkaSnapshot => Boolean): Option[KafkaSnapshot] = {
    val offset = nextOffsetFor(config.snapshotConsumerConfig, topic, config.partition)

    @annotation.tailrec
    def load(topic: String, offset: Long): Option[KafkaSnapshot] =
      if (offset < 0) None else {
        val s = snapshot(topic, offset)
        if (matcher(s)) Some(s) else load(topic, offset - 1)
      }

    load(topic, offset - 1)
  }

  /**
   * Fetches the highest sequence number for `persistenceId` from the journal actor.
    */

  private def highestJournalSequenceNr(persistenceId: String): Future[Long] = {
    val journal = extension.journalFor(null)
    implicit val timeout: Timeout = Timeout(50.seconds)
    val res = journal ? ReadHighestSequenceNr(0L, persistenceId, self)
    res.flatMap {
      case ReadHighestSequenceNrSuccess(snr) => Future.successful(snr+1)
      case ReadHighestSequenceNrFailure(err) => Future.failed(err)
    }
  }

  private def snapshot(topic: String, offset: Long): KafkaSnapshot = {
    val iter = new MessageIterator(config.snapshotConsumerConfig, topic, config.partition, offset, time.Duration.ofMillis(config.pollTimeOut))
    if(!iter.hasNext && offset>0)
      log.warning(s"Strange: Offset is not 0 ($offset), But iterator is empty. Perhaps you should increase the poll-timeout parameter (${config.pollTimeOut} ms)")
    try { serialization.deserialize(iter.next().value(), classOf[KafkaSnapshot]).get } finally { iter.close() }
  }

  private def snapshotTopic(persistenceId: String): String =
    s"${config.prefixSnapshot}${journalTopic(persistenceId)}"
}

