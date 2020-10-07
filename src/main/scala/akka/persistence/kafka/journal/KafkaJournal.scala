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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{OutOfOrderSequenceException, ProducerFencedException}

import scala.collection.JavaConverters._

private case class SeqOfAtomicWritesPromises(messages: Seq[(AtomicWrite,Promise[Unit])])
private case object CloseWriter

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer with ActorLogging {
  import context.dispatcher

  type Deletions = Map[String, (Long, Boolean)]

  val serialization: Serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))

  val journalPath: String = akka.serialization.Serialization.serializedActorPath(self)

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
  var deletions: Deletions = Map.empty

  val writers: Vector[ActorRef] = Vector.tabulate(config.writeConcurrency)(i => writer(i))

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val msgsWithPromises = messages.map{write=>(write, Promise[Unit])}

    msgsWithPromises.groupBy(msg => msg._1.persistenceId).foreach {
      case (pid,aws) => writerFor(pid)!SeqOfAtomicWritesPromises(aws)
    }

    val f = Future.sequence(msgsWithPromises.map{ case (_, p) => p.future.map(Success(_)).recover{ case e => Failure(e)}})
    f.flatMap { results =>
      val fatals = results.filter{_.isFailure}.filter {_.failed.get match {
        case _:FatalWriterException => true
        case _ => false
      }}
      if(fatals.nonEmpty) {
        Future.failed(fatals.head.failed.get.getCause)
      } else {
        f
      }
    }
  }

  private def writerFor(persistenceId: String): ActorRef =
    writers(math.abs(persistenceId.hashCode) % config.writeConcurrency)

  private def writer(index:Int): ActorRef = {
    context.actorOf(Props(new KafkaJournalWriter(journalPath,index,config,serialization)).withDispatcher(config.pluginDispatcher))
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, permanent = false))

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future.successful(readHighestSequenceNr(persistenceId, fromSequenceNr))

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = journalTopic(persistenceId)
    val highestSequenceNr = Math.max(nextOffsetFor(config.txnAwareJournalConsumerConfig, topic, config.partition)-1,0)
    if(highestSequenceNr < fromSequenceNr) {
      throw new IllegalStateException(s"Invalid highest offset: $highestSequenceNr < $fromSequenceNr")
    }
    highestSequenceNr
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    val deletions = this.deletions
    Future.successful(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, deletions, replayCallback))
  }

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, deletions: Deletions, callback: PersistentRepr => Unit): Unit = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId, (0L, false))

    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    println(s"$persistenceId replays from $fromSequenceNr/$adjustedFrom to $toSequenceNr/$adjustedTo for $adjustedNum (deletions = $deletions)")
    val iter = persistentIterator(journalTopic(persistenceId), adjustedFrom - 1L)
    iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foreach {
      p => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p)
    }
  }

  def persistentIterator(topic: String, offset: Long): Iterator[PersistentRepr] = {
    new MessageIterator(config.txnAwareJournalConsumerConfig, topic, config.partition, Math.max(offset,0),
      Duration.ofMillis(config.pollTimeOut)) .map { m =>
       println(s"Iterate on $topic at offset ${m.offset()}")
       serialization.deserialize(m.value(), classOf[PersistentRepr]).get
    }
  }
}

private class FatalWriterException(exception:Throwable) extends Throwable(exception)

private class KafkaJournalWriter(journalPath:String, index:Int,config: KafkaJournalConfig,serialization:Serialization) extends Actor with ActorLogging {
  var msgProducer: KafkaProducer[String, Array[Byte]] = createMessageProducer(journalPath,index)
  var evtProducer: KafkaProducer[String, Array[Byte]] = createEventProducer(journalPath,index)

  def receive: PartialFunction[Any, Unit] = {
    case messages: SeqOfAtomicWritesPromises =>
      writeBatchMessages(messages.messages)
    case CloseWriter =>
      msgProducer.close()
      evtProducer.close()
  }

  private def buildRecords(messages: Seq[PersistentRepr]) = {
    val recordMsgs = for {
      m <- messages
    } yield new ProducerRecord[String, Array[Byte]](journalTopic(m.persistenceId), "static", serialization.serialize(m).get)

    val recordEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- config.eventTopicMapper.topicsFor(e)
    } yield new ProducerRecord(t, e.persistenceId, serialization.serialize(e).get)

    (recordMsgs,recordEvents)
  }

  private def writeBatchMessages(batches: Seq[(AtomicWrite,Promise[Unit])]): Unit = {
    try {
      sendBatches(batches)
    } catch {
      case t:Exception => log.error(t,"unable to send atomic batch")
    }
  }

  def sendBatches(bs:Seq[(AtomicWrite,Promise[Unit])]): Unit = {
    var evtTrnBegin = false

    var abortException:Option[Throwable] = None

    msgProducer.beginTransaction()
    val results = bs.map { case (batch, p) =>
      try {
        if(abortException.isEmpty) { //An error occurs in this batch. We should not continue to send to kafka
          val (recordMsgs, recordEvents) = buildRecords(batch.payload)

          recordMsgs.foreach { recordMsg =>
            sendFuture(msgProducer, recordMsg)
          }

          if (recordEvents.nonEmpty) {
            if (!evtTrnBegin) {
              evtProducer.beginTransaction()
              evtTrnBegin = true
            }
            recordEvents.foreach { recordEvent =>
              sendFuture(evtProducer, recordEvent)
            }
          }
          (Success(),p)
        } else {
          abortException.fold[(Try[Unit],Promise[Unit])] {
            (Success(),p)
          } { exc =>
            (Failure(exc),p)
          }
        }
      } catch {
        case pfe: ProducerFencedException => log.error(pfe, "An error occurs") //Should occur only once
          rebuildProducers()
          if(abortException.isEmpty) abortException = Some(pfe)
          (Failure(new FatalWriterException(pfe)),p)
        case ooose: OutOfOrderSequenceException => log.error(ooose, "An error occurs")
          rebuildProducers()
          if(abortException.isEmpty) abortException = Some(ooose)
          (Failure(new FatalWriterException(ooose)),p)
        case nse:NotSerializableException => log.error(nse, "An error occurs")
          (Failure(nse),p)
        case e: Exception => log.error(e, "An error occurs")
          if(abortException.isEmpty) abortException = Some(e)
          (Failure(new FatalWriterException(e)),p)
      }
    }

    try {
      if (abortException.isEmpty) {
        msgProducer.commitTransaction()
        if (evtTrnBegin) {
          evtProducer.commitTransaction()
        }
      } else {
        msgProducer.abortTransaction()
        if (evtTrnBegin) {
          evtProducer.abortTransaction()
        }
      }
      results.foreach {case (r,p) => p.complete(r)}
    } catch {
      case e:Exception =>
        if(abortException.isEmpty) {
          log.error(e, "Unable to commit the transaction")
        } else {
          log.error(e, "Unable to abort the transaction")
        }
        rebuildProducers() //We have to rebuild the producers since an error on commit or abort lets the producer in error state.
        val exception = abortException.getOrElse(e)
        results.foreach {case (_,p) => p.failure(new FatalWriterException(exception))}
    }
  }

  override def postStop(): Unit = {
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }

  private def rebuildProducers():Unit = {
    msgProducer.close()
    evtProducer.close()
    //Recreate for the next batch
    msgProducer = createMessageProducer(journalPath,index)
    evtProducer = createEventProducer(journalPath,index)
  }

  private def createMessageProducer(journalPath:String, index:Int) = {
    val conf = config.journalProducerConfig() ++ Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> s"akka-journal-messages-$journalPath-$index")
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    p.initTransactions()
    p
  }

  private def createEventProducer(journalPath:String, index:Int) = {
    val conf = config.eventProducerConfig() ++ Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> s"akka-journal-events-$journalPath-$index")
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    p.initTransactions()
    p
  }
}

