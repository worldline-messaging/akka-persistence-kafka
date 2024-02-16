package akka.persistence.kafka.journal

import java.io.NotSerializableException
import java.time.Duration

import scala.collection.immutable
import scala.util.{Failure, Success, Try}
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.{Serialization, SerializationExtension}

import scala.concurrent.{Future, Promise}
import akka.actor._
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.kafka._
import akka.persistence.kafka.journal.KafkaJournalProtocol._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.jdk.CollectionConverters._

private case class SeqOfAtomicWritesPromises(messages: Seq[(AtomicWrite,Promise[Try[Unit]])])
private case object CloseWriter

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer with ActorLogging {
  import context.dispatcher

  private type Deletions = Map[String, (Long, Boolean)]

  val serialization: Serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))

  override def postStop(): Unit = {
    writers.foreach { writer =>
      writer ! CloseWriter
      writer ! PoisonPill
    }
    super.postStop()
  }

  override def receivePluginInternal: Receive = localReceive.orElse(super.receivePluginInternal)

  private def localReceive: Receive = {
    case ReadHighestSequenceNr(fromSequenceNr, persistenceId, _) =>
      try {
        val highest = readHighestSequenceNr(persistenceId, fromSequenceNr)
        sender ! ReadHighestSequenceNrSuccess(highest)
      } catch {
        case e : Exception => sender ! ReadHighestSequenceNrFailure(e)
      }
  }

  // --------------------------------------------------------------------------------------
  //  Journal writes
  // --------------------------------------------------------------------------------------

  // Transient deletions only to pass TCK (persistent not supported)
  private var deletions: Deletions = Map.empty

  private val writers: Vector[ActorRef] = Vector.tabulate(config.writeConcurrency)(_ => writer())

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val msgsWithPromises = messages.map{write=>(write, Promise[Try[Unit]])}

    msgsWithPromises.groupBy(msg => msg._1.persistenceId).foreach {
      case (pid,aws) => writerFor(pid)!SeqOfAtomicWritesPromises(aws)
    }

    val futures = msgsWithPromises.map { case (_, p) => p.future }.toList

    val future = Future.sequence(futures)

    val result = future.flatMap { results =>
      val fatals = results.filter{ result =>
        result match {
          case Failure(_:FatalWriterException) => true
          case _ => false
        }
      }
      if(fatals.nonEmpty) {
        fatals.headOption match {
          case Some(Failure(fwe:FatalWriterException)) => Future.failed(fwe.getCause)
          case _ => future
        }
      } else {
        future
      }
    }
    result
  }

  private def writerFor(persistenceId: String): ActorRef =
    writers(math.abs(persistenceId.hashCode) % config.writeConcurrency)

  private def writer(): ActorRef = {
    context.actorOf(Props(new KafkaJournalWriter(config,serialization)).withDispatcher(config.pluginDispatcher))
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr))

  private def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, false))

  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future.successful(readHighestSequenceNr(persistenceId, fromSequenceNr))

  private def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = journalTopic(persistenceId)
    val highestSequenceNr = Math.max(nextOffsetFor(config.journalConsumerConfig, topic, config.partition),0)
    if(highestSequenceNr < fromSequenceNr) {
      throw new IllegalStateException(s"Invalid highest offset: $highestSequenceNr < $fromSequenceNr")
    }
    highestSequenceNr
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    val deletions = this.deletions
    Future.successful(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, deletions, replayCallback))
  }

  private def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, deletions: Deletions, callback: PersistentRepr => Unit): Unit = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId, (0L, false))

    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    val iter = persistentIterator(journalTopic(persistenceId), adjustedFrom - 1L)
    iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(adjustedFrom) {
      case (_, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
    }

  }

  private def persistentIterator(topic: String, offset: Long): Iterator[PersistentRepr] = {
    new MessageIterator(config.journalConsumerConfig, topic, config.partition, Math.max(offset,0),
      Duration.ofMillis(config.pollTimeOut)) .map { m =>
       serialization.deserialize(m.value(), classOf[PersistentRepr]).get
    }
  }
}

private class FatalWriterException(exception:Throwable) extends Throwable(exception)

private class KafkaJournalWriter(config: KafkaJournalConfig,serialization:Serialization) extends Actor with ActorLogging {
  private val msgProducer: KafkaProducer[String, Array[Byte]] = createMessageProducer()
  private val evtProducer: KafkaProducer[String, Array[Byte]] = createEventProducer()

  def receive: PartialFunction[Any, Unit] = {
    case messages: SeqOfAtomicWritesPromises =>
      writeBatchMessages(messages.messages)
    case CloseWriter =>
      msgProducer.close()
      evtProducer.close()
  }

  private def buildRecords(messages: Seq[PersistentRepr]):
  (Seq[ProducerRecord[String, Array[Byte]]], Seq[ProducerRecord[String, Array[Byte]]]) = {
    val recordMsgs = for {
      m <- messages
    } yield new ProducerRecord[String, Array[Byte]](journalTopic(m.persistenceId), config.partition,
      None.orNull.asInstanceOf[String],
      serialization.serialize(m).get)

    val recordEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- config.eventTopicMapper.topicsFor(e)
    } yield new ProducerRecord(t, e.persistenceId, serialization.serialize(e).get)

    (recordMsgs,recordEvents)
  }

  private def writeBatchMessages(batches: Seq[(AtomicWrite,Promise[Try[Unit]])]): Unit = {
    try {
      sendBatches(batches)
    } catch {
      case t:Exception => log.error(t,"unable to send atomic batch")
    }
  }

  private def sendBatches(bs:Seq[(AtomicWrite,Promise[Try[Unit]])]): Unit = {

    import context.dispatcher

    bs.foreach { case (batch, p) =>
      if(batch.payload.size > 1) {
        p.success(Failure(new FatalWriterException(
          new UnsupportedOperationException("persistAll cannot be used with akka persistence kafka"))))
      } else {
        try {
          val (recordMsgs, recordEvents) = buildRecords(batch.payload)
          val futures = recordMsgs.map { recordMsg =>
            sendFuture(msgProducer, recordMsg)
          }
          if (recordEvents.nonEmpty) {
            recordEvents.foreach { recordEvent =>
              sendFuture(evtProducer, recordEvent)
            }
          }
          Future.sequence(futures).onComplete { r =>
            if(r.isFailure) {
              r.failed.foreach { t =>
                p.failure(t)
              }
            } else {
              p.success(Success())
            }
          }
        } catch {
          case nse: NotSerializableException => log.error(nse, "An error occurs")
            p.trySuccess(Failure(nse))
          case e: Exception => log.error(e, "An error occurs")
            p.trySuccess(Failure(new FatalWriterException(e)))
        }
      }
    }
  }

  override def postStop(): Unit = {
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }

  private def createMessageProducer() = {
    val conf = config.journalProducerConfig()
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    p
  }

  private def createEventProducer() = {
    val conf = config.eventProducerConfig()
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    p
  }
}

