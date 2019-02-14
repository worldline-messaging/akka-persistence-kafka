package akka.persistence.kafka.server

import java.util.Properties

import com.typesafe.config._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils.TestUtils

import scala.collection.JavaConverters._

object ConfigurationOverride {

  // Give a chance (mainly for tests) to override classpath application
  // configuration
  protected[kafka] var configApp: Config = _

}

object Configuration {

  def init(): Unit = {
    // We just want to initialize the configuration, which is now done
  }

  val configApp: Config = Option(ConfigurationOverride.configApp).getOrElse(ConfigFactory.load())

}

class TestServer(config: Config) extends KafkaServerTestHarness {

  private def serverProps() = {
    val serverProps = new Properties()
    config.entrySet.asScala.foreach { entry ⇒
      serverProps.put(entry.getKey, entry.getValue.unwrapped.toString)
    }
    serverProps
  }

  override def generateConfigs: Seq[KafkaConfig] = {
    Seq(TestUtils.createBrokerConfig(nodeId = 1, zkConnect = zkConnect, port = config.getInt("port")))
      .map(KafkaConfig.fromProps(_, serverProps()))
  }
}

import org.scalatest._
trait KafkaTest extends BeforeAndAfterAll { this: Suite ⇒

  var servers:List[TestServer] = List.empty

  override def beforeAll(): Unit = {
    super.beforeAll()
    if(Configuration.configApp.hasPath("test-server.instances")) {
      val serverConfig = Configuration.configApp.getConfigList("test-server.instances").asScala
      servers = serverConfig.map {
        sc => val s = new TestServer(sc); s.setUp(); s
      }.toList
    }
  }

  override def afterAll(): Unit = {
    servers.foreach{ s => s.tearDown() }
    super.afterAll()
  }
}
