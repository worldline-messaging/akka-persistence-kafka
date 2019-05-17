package akka.persistence.kafka.example.dataless

import akka.actor.{ActorSystem, Props}
import akka.persistence.PersistentActor
import com.typesafe.config.ConfigFactory
import akka.persistence._

import scala.collection.mutable
import scala.collection.mutable.Queue

sealed trait PersistedObject            extends Serializable
case class PersistId(id: Int)           extends PersistedObject
case class PersistAck(id: Long)         extends PersistedObject
case class Snapshot(oldestOffset: Long) extends PersistedObject

sealed trait Message
case class SendId(id: Int, offset: Option[Long] = None) extends Message
case class Ack(id: Int, offset: Option[Long] = None)    extends Message

class MyPersistentActor extends PersistentActor {
  var inflight: mutable.Queue[SendId] = Queue[SendId]()

  override def snapshotSequenceNr =
    inflight
      .reduceLeft { (a, b) =>
        val offsetA = a.offset.get
        val offsetB = b.offset.get
        if (offsetA < offsetB) {
          a
        } else {
          b
        }
      }
      .offset
      .get - 1

  override val persistenceId: String = "persistenceId1"

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, _) => println(s"SnapshotOffer")
    case PersistId(id) =>
      println(s"receiveRecover  : Recovering an event = PersistId($id) with sequence $lastSequenceNr")
      inflight.enqueue(SendId(id, Some(lastSequenceNr)))

    case PersistAck(id) =>
      println(s"receiveRecover  : Recovering an event = PersistAck($id) with sequence $lastSequenceNr")
      inflight = inflight.filter(e => e.id != id)

    case RecoveryCompleted => println(s"RecoveryCompleted inflight : $inflight")

    case other @ o => println(s"Unexpected receiveRecover $o")
  }

  override def receiveCommand: Receive = {
    case SendId(id, _) =>
      println(s"receiveCommand  : Received id SendId($id)")
      persist(PersistId(id)) { persistId =>
        println(s"persist callback: persistId = persistID($persistId) persisted with lastSequenceNr $lastSequenceNr")
        inflight.enqueue(SendId(persistId.id, Some(lastSequenceNr)))
        println(s"persist SendId callback: inflight = $inflight")
      }

    case Ack(id, _) =>
      println(s"receiveCommand  : Received ack Ack($id)")
      persist(PersistAck(id)) { persistAck =>
        println(
          s"persist callback: persistAck = persistAck($persistAck) persisted with lastSequenceNr $lastSequenceNr"
        )
        inflight = inflight.filter(e => e.id != persistAck.id)
        println(s"persist Ack callback: inflight = $inflight")
      }

    case "snapshot" =>
      println("snapshot")
      saveSnapshot(())
    case "kaboom" =>
      throw new Exception("exploded!")

    case SaveSnapshotSuccess(metadata) =>
      println(s"SaveSnapshotSuccess for $metadata")

    case SaveSnapshotFailure(metadata, cause) =>
      println(s"Failure at saving snapshot $metadata caused by $cause")

    case other @ o => println(s"Unexpected receiveCommand $o")
  }
}

object DatalessExample {
  def main(args: Array[String]): Unit = {
    val base   = 0
    val config = ConfigFactory.load()
    println(config.entrySet())
    val system = ActorSystem("exampleSystem", config)

    try {
      val props = Props(new MyPersistentActor)
      val p1    = system.actorOf(props, "p1")
      p1 ! SendId(1 + base)
      p1 ! SendId(2 + base)
      p1 ! SendId(3 + base)
      p1 ! Ack(1 + base)
      p1 ! Ack(2 + base)
      p1 ! "kaboom"
      p1 ! "snapshot"
      p1 ! Ack(3 + base)
      p1 ! SendId(4 + base)
      p1 ! SendId(5 + base)
      Thread.sleep(10000)
    } catch {
      case _ @e => println(e)
    } finally {
      system.terminate()
    }
  }
}
