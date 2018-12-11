package akka.persistence.kafka.journal

import akka.persistence._
import akka.persistence.journal.JournalPerfSpec
import akka.persistence.kafka.server._
import com.typesafe.config.{Config, ConfigFactory}

class KafkaJournalSpec
    extends JournalPerfSpec(
      config = ConfigFactory.parseString("""
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
    """.stripMargin)
    )
    with KafkaTest {

  override def eventsCount: Int = 10 * 10

  /** Number of measurement iterations each test will be run. */
  override def measurementIterations: Int = 10

  val systemConfig: Config = system.settings.config
  ConfigurationOverride.configApp = config.withFallback(systemConfig)

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()

  override protected def supportsSerialization: CapabilityFlag = CapabilityFlag.off()

}
