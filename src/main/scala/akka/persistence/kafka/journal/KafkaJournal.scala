package akka.persistence.kafka.journal

import java.util.Properties

import akka.actor._
import akka.pattern.ask
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka._
import akka.persistence.kafka.journal.KafkaJournalProtocol._
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import akka.util.Timeout
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

private case class SeqOfPersistentReprContainer(messages: Seq[PersistentRepr])

private case class ResultMessage(seqVal:Seq[Try[Unit]])

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer with ActorLogging {
  import context.dispatcher

  type Deletions = Map[String, (Long, Boolean)]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))

  val brokerWatcher = new BrokerWatcher(config.zookeeperConfig, self)
  var brokers = brokerWatcher.start()

  override def postStop(): Unit = {
    brokerWatcher.stop()
    super.postStop()
  }

  override def receivePluginInternal: Receive = localReceive.orElse(super.receivePluginInternal)

  private def localReceive: Receive = {
    case BrokersUpdated(newBrokers) if newBrokers != brokers =>
      brokers = newBrokers
      journalProducerConfig = config.journalProducerConfig(brokers)
      eventProducerConfig = config.eventProducerConfig(brokers)
      writers.foreach(_ ! UpdateKafkaJournalWriterConfig(writerConfig))
    case ReadHighestSequenceNr(fromSequenceNr, persistenceId, persistentActor) =>
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

  var journalProducerConfig = config.journalProducerConfig(brokers)
  var eventProducerConfig = config.eventProducerConfig(brokers)

  var writers: Vector[ActorRef] = Vector.fill(config.writeConcurrency)(writer())
  val writeTimeout = Timeout(config.requestTimeoutInMs)

  // Transient deletions only to pass TCK (persistent not supported)
  var deletions: Deletions = Map.empty

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    
    val promise = Promise[Seq[Try[Unit]]]()

    Future.sequence(messages.groupBy(_.persistenceId).map {
      case (pid,aws) => {
        val msgs = aws.map(aw => aw.payload).flatten
        writerFor(pid).ask(SeqOfPersistentReprContainer(msgs))(writeTimeout).mapTo[ResultMessage]
      }
    }.toSeq).onComplete {
      case Success(_) => promise.complete(Success(Nil)) // Nil == all good
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, false))

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  private def writerFor(persistenceId: String): ActorRef =
    writers(math.abs(persistenceId.hashCode) % config.writeConcurrency)

  private def writer(): ActorRef = {
    context.actorOf(Props(new KafkaJournalWriter(writerConfig)).withDispatcher(config.pluginDispatcher))
  }

  private def writerConfig = {
    KafkaJournalWriterConfig(journalProducerConfig, eventProducerConfig, config.eventTopicMapper, serialization)
  }


  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future(readHighestSequenceNr(persistenceId, fromSequenceNr))

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = journalTopic(persistenceId)
    leaderFor(topic, brokers) match {
      case Some(Broker(host, port)) => offsetFor(host, port, topic, config.partition)
      case None                     => fromSequenceNr
    }
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    val deletions = this.deletions
    Future(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, deletions, replayCallback))
  }

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, deletions: Deletions, callback: PersistentRepr => Unit): Unit = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId, (0L, false))

    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    val lastSequenceNr = leaderFor(journalTopic(persistenceId), brokers) match {
      case None => 0L // topic for persistenceId doesn't exist yet
      case Some(MetadataConsumer.Broker(host, port)) =>
        val iter = persistentIterator(host, port, journalTopic(persistenceId), adjustedFrom - 1L)
        iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(adjustedFrom) {
          case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
        }
    }
  }

  def persistentIterator(host: String, port: Int, topic: String, offset: Long): Iterator[PersistentRepr] = {
    val adjustedOffset = if(offset < 0) 0 else offset
    new MessageIterator(host, port, topic, config.partition, adjustedOffset, config.consumerConfig).map { m =>
      serialization.deserialize(MessageUtil.payloadBytes(m), classOf[PersistentRepr]).get
    }
  }
}

private case class KafkaJournalWriterConfig(
  journalProducerConfig: Properties,
  eventProducerConfig: Properties,
  evtTopicMapper: EventTopicMapper,
  serialization: Serialization)

private case class UpdateKafkaJournalWriterConfig(config: KafkaJournalWriterConfig)

private class KafkaJournalWriter(var config: KafkaJournalWriterConfig) extends Actor {
  var msgProducer = createMessageProducer()
  var evtProducer = createEventProducer()

  def receive = {
    case UpdateKafkaJournalWriterConfig(newConfig) =>
      msgProducer.close()
      evtProducer.close()
      config = newConfig
      msgProducer = createMessageProducer()
      evtProducer = createEventProducer()

    case messages: SeqOfPersistentReprContainer =>
      val result = writeMessages(messages.messages)
      sender ! result 
  }

  def writeMessages(messages: Seq[PersistentRepr]): Seq[Try[Unit]] = {
    val keyedMsgs = for {
      m <- messages
    } yield new ProducerRecord[String, Array[Byte]](journalTopic(m.persistenceId), "static", config.serialization.serialize(m).get)

    val keyedEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- config.evtTopicMapper.topicsFor(e)
    } yield new ProducerRecord[String, Array[Byte]](t, e.persistenceId, config.serialization.serialize(e).get)

    val journalReplies = keyedMsgs.map(record => Try(msgProducer.send(record).get()))

    keyedEvents.map(evtProducer.send)
    
    journalReplies.map(_.map(_ => {}))
  }

  override def postStop(): Unit = {
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }

  private def createMessageProducer() = new KafkaProducer[String, Array[Byte]](config.journalProducerConfig)

  private def createEventProducer() = new KafkaProducer[String, Array[Byte]](config.eventProducerConfig)
}
