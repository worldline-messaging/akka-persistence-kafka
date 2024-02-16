package akka.persistence.kafka.stress

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.persistence.{CapabilityFlag, PersistentActor, RecoveryCompleted, SnapshotOffer}
import akka.persistence.kafka.server.{ConfigurationOverride, KafkaTest}
import akka.persistence.kafka.stress.fixtures.RestartableActor
import akka.persistence.kafka.stress.fixtures.RestartableActor.RestartActor
import akka.persistence.scalatest.OptionalTests
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

import scala.util.Random

case class IncrementRequest (inc: Long, snapshot:Boolean)
case class IncrementResponse (id:String, counter:Long)
case object CounterRequest
case class CounterResponse(id:String, counter:Long)

class CounterActor(id: String) extends PersistentActor {
  var counter:Long = 0L

  override def persistenceId: String = id

  override def receiveCommand: Receive = {
    case IncrementRequest(inc,snapshot) => persistAsync(inc) { evt =>
      counter = counter + evt
      if(snapshot) {
        println(s"$id takes snapshot at $counter")
        saveSnapshot(counter)
      }
      println(s"$id sets value at $counter")
      sender() ! IncrementResponse(id, counter)
    }
    case CounterRequest => sender() ! CounterResponse(id, counter)
  }

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot: Long) ⇒ counter = snapshot; println(s"$id recovers snapshot: $counter")
    case inc: Long ⇒ {
      counter = counter + inc
      println(s"$id recovers at $counter")
    }
    case RecoveryCompleted ⇒ println(s"$id recovers with: $counter")
  }
}

class LongStressSpec extends TestKit(ActorSystem("LongStressSpec"))
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender
  with OptionalTests
  with KafkaTest{

  val config: Config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 30s
      |kafka-journal.event.producer.request.required.acks = 1
      |kafka-journal.event.producer.topic.mapper.class = "akka.persistence.kafka.EmptyEventTopicMapper"
    """.stripMargin)

  val systemConfig: Config = system.settings.config

  ConfigurationOverride.configApp = config.withFallback(systemConfig)

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def supportsLongStressTest: CapabilityFlag = CapabilityFlag.off()

  if(!supportsLongStressTest.value) {
    info("**** CapabilityFlag `supportsLongStressTest` was turned `off`. For a valid and complete test, enable the LONG STRESS TEST ****")
  }

  optional(flag = supportsLongStressTest) {
    val msBetweenSnapshot = 1000
    val msBetweenRestart = 1000
    val numberOfMessages = 100000
    val perThousandRestart = 5
    "Kafka journal" should {
      val numOfCounters =  1
      s"accept $numberOfMessages and recover work with $numOfCounters actor(s) with random poison pill and restart" in {
        var countersVal = (1 to numOfCounters).map { i =>
          (i,0L)
        }.toMap
        var counters = (1 to numOfCounters).map { i =>
          (i,system.actorOf(Props(new CounterActor(i.toString) with RestartableActor)))
        }.toMap
        var countersLastRestart = (1 to numOfCounters).map { i =>
          (i,System.currentTimeMillis())
        }.toMap
        var countersLastSnapshot = (1 to numOfCounters).map { i =>
          (i,System.currentTimeMillis())
        }.toMap
        (0L until numberOfMessages).foreach { i =>
          val counterId = Random.nextInt(numOfCounters)+1
          val counterVal = countersVal(counterId)
          val counterActor = counters(counterId)
          val counterLastRestart = countersLastRestart (counterId)
          val counterLastSnapshot = countersLastSnapshot (counterId)
          if(i%10 == 0 & i != 0) {
            println(s"Message $i")
          }

          val snapshot = if(System.currentTimeMillis() - counterLastSnapshot > msBetweenSnapshot) {
            val snapshotCounter = Random.nextInt(1000)
            if (snapshotCounter <= 10) {
              true
            } else {
              false
            }
          } else {
            false
          }
          counterActor ! IncrementRequest(1, snapshot)
          expectMsg(IncrementResponse(counterId.toString, counterVal + 1))
          if(snapshot) {
            countersLastSnapshot = countersLastSnapshot ++ Map(counterId -> (System.currentTimeMillis()))
          }
          countersVal = countersVal ++ Map(counterId -> (counterVal + 1))

          if(System.currentTimeMillis() - counterLastRestart > msBetweenRestart) {
            val restartCounter = Random.nextInt(1000)
            if (restartCounter <= perThousandRestart) {
              println(s"Restart $counterId")
              counterActor ! RestartActor
              countersLastRestart = countersLastRestart ++ Map(counterId -> (System.currentTimeMillis()))
              counterActor ! CounterRequest
              expectMsg(CounterResponse(counterId.toString, counterVal + 1))
            } else {
              val killCounter = Random.nextInt(1000)
              if (killCounter <= perThousandRestart) {
                println(s"Poison $counterId")
                counterActor ! PoisonPill
                val newCounter = system.actorOf(Props(new CounterActor(counterId.toString) with RestartableActor))
                counters = counters ++ Map(counterId -> newCounter)
                countersLastRestart = countersLastRestart ++ Map(counterId -> (System.currentTimeMillis()))
                newCounter ! CounterRequest
                expectMsg(CounterResponse(counterId.toString, counterVal + 1))
              }
            }
          }
        }
      }
    }
  }
}
