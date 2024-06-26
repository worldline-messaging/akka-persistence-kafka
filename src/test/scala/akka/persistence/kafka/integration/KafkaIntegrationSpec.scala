package akka.persistence.kafka.integration

import java.time.Duration
import java.util.UUID
import akka.actor._
import akka.persistence.JournalProtocol.{WriteMessages, WriteMessagesFailed}
import akka.persistence._
import akka.persistence.SnapshotProtocol._
import akka.persistence.kafka._
import akka.persistence.kafka.journal.KafkaJournalConfig
import akka.persistence.kafka.server._
import akka.persistence.kafka.snapshot.KafkaSnapshotStoreConfig
import akka.serialization.{Serialization, SerializationExtension, Serializer}
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import scala.collection._

object KafkaIntegrationSpec {
  val config: Config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 20s
      |kafka-journal.event.producer.request.required.acks = 1
      |kafka-journal.circuit-breaker.max-failures = 100
      |kafka-journal.circuit-breaker.call-timeout = 100s
      |kafka-journal.circuit-breaker.reset-timeout = 60s
      |akka {
      |  actor {
      |    serializers {
      |      bad-event = "akka.persistence.kafka.integration.BadEventSerializer"
      |    }
      |
      |    serialization-bindings {
      |      "akka.persistence.kafka.integration.BadEvent" = bad-event
      |    }
      |  }
      |}
    """.stripMargin)

  class TestPersistentActor(val persistenceId: String, probe: ActorRef) extends PersistentActor {
    def receiveCommand: PartialFunction[Any, Unit] = {
      case s: String =>
        persist(s)(s => probe ! s)
    }

    def receiveRecover: PartialFunction[Any, Unit] = {
      case s: SnapshotOffer =>
        probe ! s
      case s: String =>
        probe ! s
    }
  }

  /*class TestPersistentView(val persistenceId: String, val viewId: String, probe: ActorRef) extends PersistentView {
    def receive = {
      case s: SnapshotOffer =>
        probe ! s
      case s: String =>
        probe ! s
    }
  }*/
}

class BadEvent {}

class BadEventSerializer extends Serializer {
  override def identifier: Int = 35456

  override def toBinary(o: AnyRef): Array[Byte] = throw new IllegalStateException("Unable to serialize. It's a bad event")

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = throw new IllegalStateException("Unable to deserialize. It's a bad event")
}

class KafkaIntegrationSpec extends TestKit(ActorSystem("test", KafkaIntegrationSpec.config)) with ImplicitSender with AnyWordSpecLike with Matchers with KafkaTest {
  import KafkaIntegrationSpec._

  val systemConfig: Config = system.settings.config
  ConfigurationOverride.configApp = config.withFallback(systemConfig)
  val journalConfig = new KafkaJournalConfig(systemConfig.getConfig("kafka-journal"))
  val storeConfig = new KafkaSnapshotStoreConfig(systemConfig.getConfig("kafka-snapshot-store"))

  val serialization: Serialization = SerializationExtension(system)
  val eventDecoder = new EventDecoder(system)

  val persistence: Persistence = Persistence(system)
  val journal: ActorRef = persistence.journalFor(null)
  val store: ActorRef = persistence.snapshotStoreFor(null)

  override def beforeAll(): Unit = {
    super.beforeAll()
    writeJournal("pa", 1 to 3 map { i => s"a-$i" })
    writeJournal("pb", 1 to 3 map { i => s"b-$i" })
    writeJournal("pc", 1 to 3 map { i => s"c-$i" })
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  /*def withPersistentView(persistenceId: String, viewId: String)(body: ActorRef => Unit) = {
    val actor = system.actorOf(Props(new TestPersistentView(persistenceId, viewId, testActor)))
    try { body(actor) } finally { system.stop(actor) }
  }*/

  def withPersistentActor(persistenceId: String)(body: ActorRef => Unit): Unit = {
    val actor = system.actorOf(Props(new TestPersistentActor(persistenceId, testActor)))
    try { body(actor) } finally { system.stop(actor) }
  }

  def writeJournal(persistenceId: String, events: Seq[String]): Unit = withPersistentActor(persistenceId) { actor =>
    events.foreach { event => actor ! event; expectMsg(event) }
  }

  def readJournal(journalTopic: String): Seq[PersistentRepr] =
    readMessages(journalTopic, 0).map(m => serialization.deserialize(m.value(), classOf[PersistentRepr]).get)

  def readEvents(partition: Int): Seq[Event] =
    readMessages("events", partition).map(m => eventDecoder.fromBytes(m.value))

  def readMessages(topic: String, partition: Int): Seq[ConsumerRecord[String, Array[Byte]]] =
    new MessageIterator(journalConfig.journalConsumerConfig++Map(ConsumerConfig.GROUP_ID_CONFIG -> "journal-test-reader"), topic, partition, 0, Duration.ofMillis(journalConfig.pollTimeOut)).toVector

  "A Kafka journal" must {
    "publish all events to the events topic by default" in {
      val eventSeq = for {
        partition <- 0 until (if(servers.nonEmpty) servers.head.configs.head.numPartitions else  1 )
        event <- readEvents(partition)
      } yield event

      val eventMap = eventSeq.groupBy(_.persistenceId)

      eventMap("pa") should be(Seq(Event("pa", 1L, "a-1"), Event("pa", 2L, "a-2"), Event("pa", 3L, "a-3")))
      eventMap("pb") should be(Seq(Event("pb", 1L, "b-1"), Event("pb", 2L, "b-2"), Event("pb", 3L, "b-3")))
      eventMap("pc") should be(Seq(Event("pc", 1L, "c-1"), Event("pc", 2L, "c-2"), Event("pc", 3L, "c-3")))
    }
    "publish events for each persistent actor to a separate topic" in {
      readJournal("pa").map(_.payload) should be(Seq("a-1", "a-2", "a-3"))
      readJournal("pb").map(_.payload) should be(Seq("b-1", "b-2", "b-3"))
      readJournal("pc").map(_.payload) should be(Seq("c-1", "c-2", "c-3"))
    }
    "properly encode topic names" in {
      val persistenceId = "x/y/z" // not a valid topic name
      writeJournal(persistenceId, Seq("a", "b", "c"))
      readJournal(journalTopic(persistenceId)).map(_.payload) should be(Seq("a", "b", "c"))
    }
    "consider a batch as failed on fatal exception" in {
      val writerUuid = UUID.randomUUID.toString
      val msgs = (1 to 10).map { i ⇒
        PersistentRepr(payload = s"b-$i", sequenceNr = i, persistenceId = "npe", sender = Actor.noSender,
          writerUuid = writerUuid)
      }

      val probe = TestProbe()
      journal ! WriteMessages(immutable.Seq(AtomicWrite(msgs)), probe.ref, 1)


      probe.expectMsgPF() {
        case wmf:WriteMessagesFailed =>
          wmf.cause.isInstanceOf[UnsupportedOperationException] shouldBe true
          wmf.cause.getMessage shouldBe "persistAll cannot be used with akka persistence kafka"
      }
    }
  }

  "A Kafka snapshot store" when {
    "configured with ignore-orphan = true" must {
      "ignore orphan snapshots (snapshot sequence nr > highest journal sequence nr)" in {
        storeConfig.ignoreOrphan shouldBe true
        val persistenceId = "pa"

        store ! SaveSnapshot(SnapshotMetadata(persistenceId, 7), "test")
        expectMsgPF() { case SaveSnapshotSuccess(md) => md.sequenceNr should be(7L) }

        withPersistentActor(persistenceId) { _ =>
          expectMsg("a-1")
          expectMsg("a-2")
          expectMsg("a-3")
        }
      }
      "fallback to non-orphan snapshots" in {
        val persistenceId = "pa"

        store ! SaveSnapshot(SnapshotMetadata(persistenceId, 2), "test")
        expectMsgPF() { case SaveSnapshotSuccess(md) => md.sequenceNr should be(2L) }

        store ! SaveSnapshot(SnapshotMetadata(persistenceId, 7), "test")
        expectMsgPF() { case SaveSnapshotSuccess(md) => md.sequenceNr should be(7L) }

        withPersistentActor(persistenceId) { _ =>
          expectMsgPF() { case SnapshotOffer(SnapshotMetadata(_, snr, _), _) => snr should be(2) }
          expectMsg("a-3")
        }
      }
      /*"not ignore view snapshots (for which no corresponding journal topic exists)" in {
        val persistenceId = "pa"
        val viewId = "va"

        store ! SaveSnapshot(SnapshotMetadata(viewId, 2), "test")
        expectMsgPF() { case SaveSnapshotSuccess(md) => md.sequenceNr should be(2L) }

        withPersistentView(persistenceId, viewId) { _ =>
          expectMsgPF() { case SnapshotOffer(SnapshotMetadata(_, snr, _), _) => snr should be(2) }
          expectMsg("a-3")
        }
      }*/
    }
  }
}
