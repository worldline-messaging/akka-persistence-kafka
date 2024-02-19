package akka.persistence.kafka.stress.shopping

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import ShoppingCartActor._
import akka.persistence.kafka.server.{ConfigurationOverride, KafkaTest}
import akka.persistence.kafka.stress.fixtures.RestartableActor
import akka.persistence.kafka.stress.fixtures.RestartableActor._
import com.typesafe.config.{Config, ConfigFactory}


//Just an integration of https://github.com/tudorzgureanu/akka-persistence-playground.git

class ShoppingCartActorSpec2
  extends TestKit(ActorSystem("ShoppingCartActorSpec2"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender
    with KafkaTest {

  val config: Config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 20s
      |kafka-journal.event.producer.request.required.acks = 1
      |kafka-journal.event.producer.topic.mapper.class = "akka.persistence.kafka.EmptyEventTopicMapper"
    """.stripMargin)

  val systemConfig: Config = system.settings.config

  ConfigurationOverride.configApp = config.withFallback(systemConfig)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ShoppingCartActor" should {
    val shoppingItem = ShoppingItem("sku-000001", "Cheap headphones", 42.25, 2)

    "add an item to the shopping cart and preserve it after restart" in {
      val shoppingCartActor = system.actorOf(Props(new ShoppingCartActor("sc-000001") with RestartableActor))

      shoppingCartActor ! AddItemCommand(shoppingItem)
      expectMsg(AddItemResponse(shoppingItem))

      shoppingCartActor ! RestartActor
      shoppingCartActor ! GetItemsRequest

      expectMsg(GetItemsResponse(Seq(shoppingItem)))
    }

    "update an existing item to the shopping cart and preserve the changes after restart" in {
      val shoppingCartActor = system.actorOf(Props(new ShoppingCartActor("sc-000002") with RestartableActor))
      val updatedShoppingItem = shoppingItem.copy(quantity = 5)

      shoppingCartActor ! AddItemCommand(shoppingItem)
      expectMsg(AddItemResponse(shoppingItem))
      shoppingCartActor ! UpdateItemCommand(updatedShoppingItem)
      expectMsg(UpdateItemResponse(updatedShoppingItem))

      shoppingCartActor ! RestartActor
      shoppingCartActor ! GetItemsRequest

      expectMsg(GetItemsResponse(Seq(updatedShoppingItem)))
    }

    "remove an existing item from the shopping cart and preserve the changes after restart" in {
      val shoppingCartActor = system.actorOf(Props(new ShoppingCartActor("sc-000003") with RestartableActor))

      shoppingCartActor ! AddItemCommand(shoppingItem)
      expectMsg(AddItemResponse(shoppingItem))
      shoppingCartActor ! RemoveItemCommand(shoppingItem.id)
      expectMsg(RemoveItemResponse(shoppingItem.id))

      shoppingCartActor ! RestartActor
      shoppingCartActor ! GetItemsRequest

      expectMsg(GetItemsResponse(Seq.empty))
    }
  }
}
