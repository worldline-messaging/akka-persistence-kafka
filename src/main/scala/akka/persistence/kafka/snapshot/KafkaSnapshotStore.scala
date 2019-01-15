package akka.persistence.kafka.snapshot

import akka.actor._
import akka.pattern.ask
import akka.persistence._
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.kafka._
import akka.serialization.SerializationExtension
import akka.util.Timeout
import akka.persistence.kafka.journal.KafkaJournalProtocol._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.control.NonFatal

class KafkaSnapshotStore extends SnapshotStore with MetadataConsumer with ActorLogging {
  import context.dispatcher

  val extension = Persistence(context.system)

  type RangeDeletions  = Map[String, SnapshotSelectionCriteria]
  type SingleDeletions = Map[String, List[SnapshotMetadata]]

  val serialization = SerializationExtension(context.system)
  val config        = new KafkaSnapshotStoreConfig(context.system.settings.config.getConfig("kafka-snapshot-store"))

  val snapshotProducer = new KafkaProducer[String, Array[Byte]](config.producerConfig().asJava)

  override def postStop(): Unit = {
    super.postStop()
  }

  // Transient deletions only to pass TCK (persistent not supported)
  var rangeDeletions: RangeDeletions   = Map.empty.withDefaultValue(SnapshotSelectionCriteria.None)
  var singleDeletions: SingleDeletions = Map.empty.withDefaultValue(Nil)

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful {
    rangeDeletions += (persistenceId → criteria)
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful {
    singleDeletions.get(metadata.persistenceId) match {
      case Some(dels) ⇒ singleDeletions += (metadata.persistenceId → (metadata :: dels))
      case None       ⇒ singleDeletions += (metadata.persistenceId → List(metadata))
    }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val snapshotBytes = serialization.serialize(KafkaSnapshot(metadata, snapshot)).get
    val snapshotMessage =
      new ProducerRecord[String, Array[Byte]](snapshotTopic(metadata.persistenceId), "static", snapshotBytes)
    sendFuture(snapshotProducer, snapshotMessage)
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val singleDeletions = this.singleDeletions
    val rangeDeletions  = this.rangeDeletions

    for {
      highest ← if (config.ignoreOrphan) highestJournalSequenceNr(persistenceId) else Future.successful(Long.MaxValue)
      adjusted = if (config.ignoreOrphan &&
                     highest < criteria.maxSequenceNr &&
                     highest > 0L) criteria.copy(maxSequenceNr = highest)
      else criteria
      // highest  <- Future.successful(Long.MaxValue)
      // adjusted = criteria
      snapshot ← Future {
        val topic = snapshotTopic(persistenceId)
        // if timestamp was unset on delete, matches only on same sequence nr
        def matcher(snapshot: KafkaSnapshot): Boolean =
          snapshot.matches(adjusted) &&
            !snapshot.matches(rangeDeletions(persistenceId)) &&
            !singleDeletions(persistenceId).contains(snapshot.metadata) &&
            !singleDeletions(persistenceId)
              .filter(_.timestamp == 0L)
              .map(_.sequenceNr)
              .contains(snapshot.metadata.sequenceNr)

        load(topic, matcher).map(s ⇒ SelectedSnapshot(s.metadata, s.snapshot))
      }
    } yield snapshot
  }

  def load(topic: String, matcher: KafkaSnapshot ⇒ Boolean): Option[KafkaSnapshot] = {
    val offset = nextOffsetFor(config.snapshotConsumerConfig, topic, config.partition)

    @annotation.tailrec
    def load(topic: String, offset: Long): Option[KafkaSnapshot] =
      if (offset < 0) None
      else {
        val s = snapshot(topic, offset)
        if (matcher(s)) Some(s) else load(topic, offset - 1)
      }

    load(topic, offset - 1)
  }

  /**
    * Fetches the highest sequence number for `persistenceId` from the journal actor.
    */
  private def highestJournalSequenceNr(persistenceId: String): Future[Long] = {
    val journal                   = extension.journalFor(null)
    implicit val timeout: Timeout = config.readHighestSequenceNrTimeout
    val res                       = journal ? ReadHighestSequenceNr(0L, persistenceId, self)
    res.flatMap {
      case ReadHighestSequenceNrSuccess(snr) ⇒ Future.successful(snr + 1)
      case ReadHighestSequenceNrFailure(err) ⇒ Future.failed(err)
    }
  }

  private def snapshot(topic: String, offset: Long): KafkaSnapshot = {
    var retries                       = config.failedSnapshotRetries
    var result: Option[KafkaSnapshot] = None
    while (retries > 0 && result.isEmpty) {
      val iter = new MessageIterator(config.snapshotConsumerConfig, topic, config.partition, offset, config.pollTimeOut)
      if (!iter.hasNext && offset > 0)
        log.warning(
          s"Strange: Offset is not 0 ($offset), But iterator is empty. Perhaps you should increase the poll-timeout parameter (${config.pollTimeOut} ms)"
        )
      try {
        result = Some(serialization.deserialize(iter.next().value(), classOf[KafkaSnapshot]).get)
      } catch {
        case NonFatal(e) ⇒
          retries -= 1
          if (retries == 0) {
            log.error(e, "could not read snapshot")
            throw e
          }
      } finally {
        iter.close()
      }
    }
    result.get
  }

  private def snapshotTopic(persistenceId: String): String =
    s"${config.prefix}${journalTopic(persistenceId)}"
}
