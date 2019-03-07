package akka.persistence.kafka.integration

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.persistence.kafka.integration.KafkaIntegrationSpec.config
import akka.persistence.kafka.server.{Configuration, ConfigurationOverride, TestServer}
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.JavaConverters._

object BenchCoherencePersistentActor {
  case object Write
  case object Compute

  def props(id:Int, messageStart:String, snapshotStart:String, gounter:ActorRef, latch: CountDownLatch, si:Int): Props = Props(new BenchCoherencePersistentActor(id,messageStart,snapshotStart,gounter, latch, si))
}

object CounterActor {
  case object Add
  case object Get
}

class CounterActor extends Actor with ActorLogging {
  import CounterActor._
  var cpt = 0L
  override def receive: Receive = {
    case Add => cpt = cpt + 1
    case Get => sender() ! cpt
  }
}

case class Snapshot(name:String, data:String)

/**
  * Created by giena on 16/11/17.
  */
class BenchCoherencePersistentActor(id:Int, messageStart:String, snapshotStart:String, gounter:ActorRef, latch: CountDownLatch, si:Int) extends PersistentActor with ActorLogging {
  import BenchCoherencePersistentActor._
  import CounterActor._
  var startMsg = 0L
  var lastMsg = 0L
  var startRecov = 0L
  var timeRecov = 0L
  var cptMsg = 0L
  var cptRecov = 0L
  var snaps = 0L

  var numRecov = 0L

  var firstValueRecov = 0L
  var lastValueRecov = 0L

  override def persistenceId: String = s"bench-persist-$id"

  override def receiveRecover: Receive = {
    case SnapshotOffer(metadata,value:Snapshot) =>
      val d = value.data.toLong
      println(s"RECOVERING ${value.name} $d with $metadata at $lastSequenceNr")
      if(startRecov==0L)
        startRecov = System.currentTimeMillis()
      cptRecov = d
    case RecoveryCompleted => println("RECOVERED"); latch.countDown()
      if(startRecov==0) timeRecov=0 else timeRecov = System.currentTimeMillis()-startRecov;
      lastValueRecov = cptRecov
      println(s"[$firstValueRecov:$lastValueRecov:$numRecov:$cptRecov:$timeRecov]")
      cptMsg = cptRecov
    case msg:String =>
      if(startRecov==0L) startRecov = System.currentTimeMillis()
      numRecov = numRecov + 1
      val newValue = msg.toLong
      if(firstValueRecov == 0L) firstValueRecov = newValue
      if(newValue-cptRecov > 1) throw new IllegalStateException(s"$persistenceId with $newValue-$cptRecov = ${newValue-cptRecov}");
      cptRecov = newValue
  }

  override def receiveCommand: Receive = {
    case Write =>
      if(startMsg==0L) startMsg=System.currentTimeMillis()
      persistAsync(messageStart+cptMsg) { _ =>
        gounter ! Add
        lastMsg = System.currentTimeMillis()
        cptMsg = cptMsg + 1
        if(cptMsg!=0 && cptMsg%si==0) {
          saveSnapshot(Snapshot(s"snapshot $snaps at $cptMsg", snapshotStart + cptMsg))
          snaps = snaps + 1
        }
      }

    case Compute =>
      val time = lastMsg-startMsg
      sender() ! s"[$cptMsg:$snaps:$time][$numRecov:$timeRecov]"
  }
}

object PerfActor extends App  {
  import BenchCoherencePersistentActor._
  import CounterActor._
  import Math._
  val nm = 2000000
  val na = 1
  val randomWrite = false //false = RR
  val ml = 1000
  val sl = 1000*10
  val si = 150000
  val ti = 50000

  val config: Config = ConfigFactory.parseString(
    """
      |kafka-journal.circuit-breaker.max-failures = 10
      |kafka-journal.circuit-breaker.call-timeout = 60s
      |kafka-journal.circuit-breaker.reset-timeout = 120s
    """.stripMargin)


  val configApp = config.withFallback(ConfigFactory.load())

  val servers = if(configApp.hasPath("test-server.instances")) {
    val serverConfig = configApp.getConfigList("test-server.instances").asScala
    serverConfig.map {
      sc => val s = new TestServer(sc); s.setUp(); s
    }.toList
  } else {
    List.empty
  }

  val latch = new CountDownLatch(na)

  val startMsg = List.fill(ml)('0').mkString// s"%0${messageLength}d".format(cpt)
  println(startMsg.size)
  val startSnpsht = List.fill(sl)('0').mkString//s"%0${snapshotLength}d".format(cpt)
  println(startSnpsht.size)

  val system = ActorSystem("test-magic-system",Some(configApp))

  val gounter = system.actorOf(Props(new CounterActor))

  var actors = Vector.tabulate(na)(id => system.actorOf(props(id,startMsg,startSnpsht,gounter,latch,si)))


  latch.await()

  println(s"Writing $nm messages to $na actors ($randomWrite,$ml,$sl,$si)")
  (1 to nm).foreach { i =>
    val index = if(randomWrite) abs(Random.nextInt())%na else i%na
    val ref = actors(index)
    ref ! Write
    if(i%ti==0) Thread.sleep(1000)
  }

  implicit val executionContext = system.dispatchers.lookup("my-dispatcher")

  implicit val timeout = Timeout(5 seconds)

  var cpt = 0L
  do {
    cpt =  Await.result(gounter ask(Get),5 seconds).asInstanceOf[Long]
    if(cpt < nm)
      Thread.sleep(5000)
    print(cpt+" ")
  } while(cpt!=nm)


  println("End Writing")

  println("Compute results")
  (0 until na).foreach { id =>
    val response = Await.result(actors(id) ask Compute, 5 seconds)
    println(s"Actor $id. $response.")
  }

  servers.foreach {s => s.tearDown()}
  system.terminate()

}
