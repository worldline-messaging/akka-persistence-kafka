package akka.persistence.kafka.integration

import java.time.Duration
import java.util.{Properties, UUID}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.persistence.JournalProtocol.{WriteMessages, WriteMessagesFailed}
import akka.persistence.kafka.integration.KafkaIntegrationSpec.TestPersistentActor
import akka.persistence.kafka.journal.KafkaJournalConfig
import akka.persistence.{AtomicWrite, Persistence, PersistentRepr}
import akka.persistence.kafka.{EventDecoder, MessageIterator}
import akka.serialization.{Serialization, SerializationExtension}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.errors.TimeoutException
import org.junit.Test
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

class KafkaServerTest extends ZooKeeperTestHarness {
  def createServer(nodeId:Int, port:Int, serverProps:Properties = new Properties()): KafkaServer = {
    val props = TestUtils.createBrokerConfig(nodeId = nodeId, zkConnect = zkConnect, port = port)
    val kafkaConfig = KafkaConfig.fromProps(props, serverProps)

    TestUtils.createServer(kafkaConfig)
  }

  @Test
  def testAlreadyRegisteredAdvertisedListeners() {
    //start a server with a advertised listener
    val server1 = createServer(1, TestUtils.RandomPort)

    //start a server with same advertised listener
    intercept[IllegalArgumentException] {
      createServer(2, TestUtils.boundPort(server1))
    }

    //start a server with same host but with different port
    val server2 = createServer(2, TestUtils.RandomPort)

    TestUtils.shutdownServers(Seq(server1, server2))
  }

}

object KafkaClusterFailureSpec {
  val config: Config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |kafka-journal.producer.bootstrap.servers = "localhost:6668,localhost:6669,localhost:6670"
      |kafka-journal.event.producer.bootstrap.servers = "localhost:6668,localhost:6669,localhost:6670"
      |kafka-journal.consumer.bootstrap.servers = "localhost:6668,localhost:6669,localhost:6670"
      |kafka-journal.consumer.poll-timeout = 10000
      |
      |kafka-snapshot-store.producer.bootstrap.servers = "localhost:6668,localhost:6669,localhost:6670"
      |kafka-snapshot-store.consumer.bootstrap.servers = "localhost:6668,localhost:6669,localhost:6670"
      |kafka-snapshot-store.consumer.poll-timeout = 10000
      |
      |akka.test.single-expect-default = 20s
      |kafka-journal.event.producer.topic.mapper.class = "akka.persistence.kafka.EmptyEventTopicMapper"
      |
      |kafka-journal.producer.retries = 5
      |kafka-journal.producer.delivery.timeout.ms = 2000
      |kafka-journal.producer.request.timeout.ms = 1000
      |
      |kafka-journal.circuit-breaker.max-failures = 10
      |kafka-journal.circuit-breaker.call-timeout = 30s
      |kafka-journal.circuit-breaker.reset-timeout = 60s
    """.stripMargin)
}
class KafkaClusterFailureSpec extends TestKit(ActorSystem("test", KafkaClusterFailureSpec.config)) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {
  import kafka.utils.TestUtils._

  import KafkaClusterFailureSpec._

  val systemConfig: Config = system.settings.config
  val configApp: Config = config.withFallback(systemConfig)
  val journalConfig = new KafkaJournalConfig(systemConfig.getConfig("kafka-journal"))

  val serialization: Serialization = SerializationExtension(system)
  val eventDecoder = new EventDecoder(system)

  val kafkaServerTest = new KafkaServerTest

  val serverConfig: Config = configApp.getConfigList("test-server.instances").asScala.head
  val serverProps = new Properties()
  serverConfig.entrySet.asScala.foreach { entry ⇒
    val invalids = List("broker.id","port","log.dirs")
    if(!invalids.contains(entry.getKey)) {
      serverProps.put(entry.getKey, entry.getValue.unwrapped.toString)
    }
  }
  serverProps.put("default.replication.factor","3")
  serverProps.put("transaction.state.log.replication.factor","3")
  serverProps.put("transaction.state.log.min.isr","2")
  serverProps.put("offsets.topic.replication.factor","3")
  var servers:Seq[KafkaServer] = List.empty

  val persistence: Persistence = Persistence(system)
  val journal: ActorRef = persistence.journalFor(null)
  val store: ActorRef = persistence.snapshotStoreFor(null)


  override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaServerTest.setUp()
    servers = (0 to 2).map { i => kafkaServerTest.createServer(i+100, 6668+i, serverProps) }
  }

  override def afterAll(): Unit = {
    shutdownServers(servers)
    kafkaServerTest.tearDown()
    system.terminate()
    super.afterAll()
  }

  def writeJournal(events: Seq[String], actor:ActorRef): Unit = {
    events.foreach { event => actor ! event; expectMsg(event) }
  }

  def readJournal(journalTopic: String): Seq[PersistentRepr] =
    readMessages(journalTopic, 0).map(m => serialization.deserialize(m.value(), classOf[PersistentRepr]).get)

  def readMessages(topic: String, partition: Int): Seq[ConsumerRecord[String, Array[Byte]]] =
    new MessageIterator(journalConfig.txnAwareJournalConsumerConfig++Map(ConsumerConfig.GROUP_ID_CONFIG -> "journal-test-reader"), topic, partition, 0, Duration.ofMillis(journalConfig.pollTimeOut)).toVector

  "A kafka journal" must {
    "properly manage all nodes shutdown on three node cluster" in {
      val persistenceId = "pshutdown"
      val actor = system.actorOf(Props(new TestPersistentActor(persistenceId, testActor)))
      val writerUuid = UUID.randomUUID.toString

      writeJournal(Seq("a", "b", "c"), actor)
      servers.foreach(_.shutdown())

      val probe: TestProbe = TestProbe()

      val msgs = (1 to 10).map { _ ⇒
        PersistentRepr(payload = "1", sequenceNr = 3, persistenceId = persistenceId, sender = Actor.noSender,
          writerUuid = writerUuid)
      }
      journal ! WriteMessages(Seq(AtomicWrite(msgs)), probe.ref, 1)

      Thread.sleep(10000)
      servers.foreach(_.startup())

      probe.expectMsgPF() {
        case wmf:WriteMessagesFailed =>
          wmf.cause.isInstanceOf[TimeoutException] shouldBe true
          wmf.cause.getMessage.startsWith("Expiring 10 record(s) for pshutdown") shouldBe true
      }

      readJournal(persistenceId).map(_.payload) should be(Seq("a", "b", "c"))
      writeJournal(Seq("d", "e", "f"), actor)
      readJournal(persistenceId).map(_.payload) should be(Seq("a", "b", "c", "d", "e", "f"))
    }

    "properly manage one node shutdown on three node cluster" in {
      val persistenceId = "pshutdown2"
      val actor = system.actorOf(Props(new TestPersistentActor(persistenceId, testActor)))

      writeJournal(Seq("a", "b", "c"), actor)
      readJournal(persistenceId).map(_.payload) should be(Seq("a", "b", "c"))

      val leader = servers.find(TestUtils.isLeaderLocalOnBroker(persistenceId, 0, _))

      leader.isDefined shouldBe true

      leader.foreach { _.shutdown() }

      writeJournal(Seq("d", "e", "f"), actor)
      readJournal(persistenceId).map(_.payload) should be(Seq("a", "b", "c", "d", "e", "f"))

      leader.foreach(_.startup())
    }
  }
}
